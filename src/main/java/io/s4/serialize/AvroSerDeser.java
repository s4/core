/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.serialize;

import static io.s4.collector.Event.EVENT_NAME_KEY;
import io.s4.schema.AvroSchemaManager;
import io.s4.util.MiscConstants;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.util.Utf8;

public class AvroSerDeser implements SerializerDeserializer {

    private AvroSchemaManager avroSchemaManager;

    public void setAvroSchemaManager(AvroSchemaManager avroSchemaManager) {
        this.avroSchemaManager = avroSchemaManager;
    }

    @Override
    public Object deserialize(byte[] rawMessage) {
        // convert the byte array into an event object
        Map<String, Object> event = null;
        Schema wrapperSchema = avroSchemaManager.getCompiledSchema(MiscConstants.EVENT_WRAPPER_SCHEMA_NAME);
        GenericRecord wrapper = new GenericData.Record(wrapperSchema);

        try {
            wrapper = deserialize(wrapperSchema, rawMessage);

            Utf8 schemaNameUtf8 = (Utf8) wrapper.get("eventType");
            if (schemaNameUtf8 == null) {
                throw new RuntimeException("Wrapper message does not contain eventType field");
            }

            String schemaName = schemaNameUtf8.toString();
            Schema eventSchema = avroSchemaManager.getCompiledSchema(schemaName);

            ByteBuffer byteBuffer = (ByteBuffer) wrapper.get("rawdata");
            if (byteBuffer == null) {
                throw new RuntimeException("Wrapper message does not contain rawdata field");
            }

            byte[] byteData = byteBuffer.array();
            GenericRecord avroEvent = deserialize(eventSchema, byteData);

            // convert the avro version of the event into a Map
            event = new HashMap<String, Object>();
            copyRecord(avroEvent, event);
            if (event.get(EVENT_NAME_KEY) == null) {
                event.put(EVENT_NAME_KEY, schemaName);
            }
            return event;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public byte[] serialize(Object message) {
        Map<String, Object> event = (Map<String, Object>) message;

        Schema wrapperSchema = avroSchemaManager.getCompiledSchema(MiscConstants.EVENT_WRAPPER_SCHEMA_NAME);
        GenericRecord wrapper = new GenericData.Record(wrapperSchema);

        String eventName = (String) event.get(io.s4.collector.Event.EVENT_NAME_KEY);
        String schemaName = eventName;
        wrapper.put("eventType", new Utf8(schemaName));

        if (event.get("traceId") != null) {
            wrapper.put("traceId", event.get("traceId"));
        } else {
            wrapper.put("traceId", new Long(-1));
        }

        Schema eventSchema = avroSchemaManager.getCompiledSchema(schemaName);
        GenericRecord avroRecord = new GenericData.Record(eventSchema);
        copyRecord(event, eventSchema, avroRecord);
        try {
            byte[] serializedEvent = serialize(eventSchema, avroRecord);

            ByteBuffer byteBuffer = ByteBuffer.allocate(serializedEvent.length);
            byteBuffer.put(serializedEvent);
            byteBuffer.rewind();

            // put the serialized event in the wrapper
            wrapper.put("rawdata", byteBuffer);

            // serialize the wrapper for transmission
            return serialize(wrapperSchema, wrapper);

        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static GenericRecord deserialize(Schema schema, byte[] array)
            throws IOException {
        GenericDatumReader<GenericRecord> serveReader = new GenericDatumReader<GenericRecord>(schema);
        return serveReader.read(null,
                                new BinaryDecoder(new ByteArrayInputStream(array)));
    }

    public static byte[] serialize(Schema schema, GenericRecord content)
            throws IOException {
        GenericDatumWriter<GenericRecord> serveWriter = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serveWriter.write(content, new BinaryEncoder(out));
        return out.toByteArray();
    }

    public static Map<String, Object> copyRecord(GenericRecord avroRecord, Map<String, Object> record) {
        for (String fieldName : avroRecord.keySet()) {
            Object value = avroRecord.get(fieldName);
            if (value == null) {
                continue; // it is expected that the key set will point to null
                          // values
            }

            if (value instanceof GenericRecord) {
                record.put(fieldName,
                           copyRecord((GenericRecord) value,
                                      new HashMap<String, Object>()));
            } else if (value instanceof GenericArray) {
                GenericArray avroArray = (GenericArray) value;
                List<Map<String, Object>> list = new ArrayList<Map<String, Object>>((int) avroArray.size());
                record.put(fieldName, list);
                copyArray(avroArray, list);
            } else if (value instanceof Utf8) {
                record.put(fieldName, ((Utf8) value).toString());
            } else {
                record.put(fieldName, value);
            }
        }
        return record;
    }

    public static List<Map<String, Object>> copyArray(GenericArray<GenericRecord> avroArray, List<Map<String, Object>> list) {
        for (GenericRecord avroRecord : avroArray) {
            Map<String, Object> record = new HashMap<String, Object>();
            list.add(copyRecord(avroRecord, record));
        }
        return list;
    }

    public static GenericRecord copyRecord(Map<String, Object> record, Schema schema, GenericRecord avroRecord) {
        Map<String, Field> fieldMap = schema.getFields();
        for (String fieldName : record.keySet()) {
            Field field = fieldMap.get(fieldName);
            if (field == null) {
                continue; // not in schema, ignore
            }

            Schema fieldSchema = field.schema();

            if (fieldSchema.getType().equals(Schema.Type.UNION)) {
                List<Schema> actualSchemas = fieldSchema.getTypes();
                // this should only contain two entries, the actual type and
                // NULL
                fieldSchema = null;
                for (Schema actualSchema : actualSchemas) {
                    if (actualSchema.getType().equals(Schema.Type.NULL)) {
                        continue;
                    }
                    fieldSchema = actualSchema;
                    break;
                }

                if (fieldSchema == null) {
                    return avroRecord;
                }
            }

            Object value = record.get(fieldName);
            if (value == null) {
                continue;
            }
            if (fieldSchema.getType().equals(Schema.Type.STRING)) {
                avroRecord.put(fieldName, new Utf8((String) value));
            } else if (fieldSchema.getType().equals(Schema.Type.RECORD)) {
                if (!(value instanceof Map)) {
                    continue; // schema mismatch
                }
                avroRecord.put(fieldName,
                               copyRecord((Map<String, Object>) record.get(fieldName),
                                          fieldSchema,
                                          new GenericData.Record(fieldSchema)));
            } else if (fieldSchema.getType().equals(Schema.Type.ARRAY)) {
                if (!(value instanceof List)) {
                    continue; // schema mismatch
                }
                List<Map<String, Object>> list = (List<Map<String, Object>>) record.get(fieldName);
                GenericArray<GenericRecord> avroArray = new GenericData.Array<GenericRecord>(list.size());
                avroRecord.put(fieldName, avroArray);
                copyArray(list, fieldSchema.getElementType(), avroArray);
            } else if (fieldSchema.getType().equals(Schema.Type.ENUM)
                    || fieldSchema.getType().equals(Schema.Type.UNION)
                    || fieldSchema.getType().equals(Schema.Type.BYTES)
                    || fieldSchema.getType().equals(Schema.Type.MAP)) {
                continue; // we don't properly handle that right now
            } else {
                avroRecord.put(fieldName, value);
            }
        }
        return avroRecord;
    }

    public static GenericArray<GenericRecord> copyArray(List<Map<String, Object>> list, Schema elementSchema, GenericArray<GenericRecord> avroArray) {
        if (!elementSchema.getType().equals(Schema.Type.RECORD)) {
            return avroArray; // something weird here, we only support array of
                              // records
        }

        for (Map<String, Object> record : list) {
            GenericRecord avroRecord = new GenericData.Record(elementSchema);
            avroArray.add(avroRecord);
            copyRecord(record, elementSchema, avroRecord);
        }
        return avroArray;
    }
}
