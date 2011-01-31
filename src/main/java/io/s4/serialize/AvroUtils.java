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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;

public class AvroUtils {
    public static GenericRecord deserialize(Schema schema, byte[] array)
            throws Exception {
        GenericDatumReader<GenericRecord> serveReader = new GenericDatumReader<GenericRecord>(schema);
        return serveReader.read(null,
                                new BinaryDecoder(new ByteArrayInputStream(array)));
    }

    public static byte[] serialize(Schema schema, GenericRecord content)
            throws Exception {
        GenericDatumWriter<GenericRecord> serveWriter = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serveWriter.write(content, new BinaryEncoder(out));
        return out.toByteArray();
    }
}
