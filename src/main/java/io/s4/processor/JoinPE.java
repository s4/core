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
package io.s4.processor;

import io.s4.collector.Event;
import io.s4.collector.EventRecord;
import io.s4.dispatcher.Dispatcher;
import io.s4.logger.Monitor;
import io.s4.schema.Schema;
import io.s4.schema.SchemaContainer;
import io.s4.schema.Schema.Property;
import io.s4.util.MetricsName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import static io.s4.util.MetricsName.*;

public class JoinPE extends AbstractPE {
    private static Logger logger = Logger.getLogger(JoinPE.class);
    private Map<String, List<String>> eventFields = new HashMap<String, List<String>>();
    private Map<String, Object> eventsToJoin;
    private Dispatcher dispatcher;
    private Monitor monitor;
    private String id = "JoinPE";
    private String outputStreamName;
    private String outputClassName;
    private Class<?> outputClass;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    public void setOutputStreamName(String outputStreamName) {
        this.outputStreamName = outputStreamName;
    }

    public String getOutputClassName() {
        return outputClassName;
    }

    public void setOutputClassName(String outputClassName) {
        this.outputClassName = outputClassName;
        try {
            this.outputClass = Class.forName(this.outputClassName);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(cnfe);
        }
    }

    public void setIncludeFields(String[] includeFields) {
        for (String includeField : includeFields) {
            StringTokenizer st = new StringTokenizer(includeField);
            if (st.countTokens() != 2) {
                Logger.getLogger("s4").error("Bad include field specified: "
                        + includeField);
                continue;
            }

            String eventName = st.nextToken();
            String fieldName = st.nextToken();

            List<String> fieldNames = eventFields.get(eventName);
            if (fieldNames == null) {
                fieldNames = new ArrayList<String>();
                eventFields.put(eventName, fieldNames);
            }

            if (fieldName.equals("*")) {
                fieldNames.clear();
                fieldNames.add("*");
            } else {
                fieldNames.add(fieldName);
            }
        }
    }

    @Override
    public void output() {
        // TODO Auto-generated method stub

    }

    private SchemaContainer schemaContainer = new SchemaContainer();

    public void processEvent(Object event) {
        if (eventsToJoin == null) {
            eventsToJoin = new HashMap<String, Object>();
        }
        List<String> fieldNames = eventFields.get(getStreamName());
        if (fieldNames == null) {
            return;
        }

        // we only use the last event that comes through on the given stream
        eventsToJoin.put(getStreamName(), event);

        if (eventsToJoin.keySet().size() == eventFields.keySet().size()) {
            Object newEvent = null;
            try {
                newEvent = outputClass.newInstance();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            Schema newEventSchema = schemaContainer.getSchema(newEvent.getClass());

            for (String streamName : eventsToJoin.keySet()) {
                Object partialEvent = eventsToJoin.get(streamName);
                Schema partialEventSchema = schemaContainer.getSchema(partialEvent.getClass());

                List<String> includeFields = eventFields.get(streamName);
                if (includeFields.size() == 1
                        && includeFields.get(0).equals("*")) {
                    for (Property partialEventProperty : partialEventSchema.getProperties()
                                                                           .values()) {
                        copyField(partialEventProperty.getName(),
                                  partialEventSchema,
                                  newEventSchema,
                                  partialEvent,
                                  newEvent);
                    }
                } else {
                    for (String includeField : includeFields) {
                        copyField(includeField,
                                  partialEventSchema,
                                  newEventSchema,
                                  partialEvent,
                                  newEvent);
                    }
                }
            }

            dispatcher.dispatchEvent(outputStreamName, newEvent);
            if (logger.isDebugEnabled()) {
                logger.debug("STEP 7 (JoinPE): " + newEvent.toString());
            }
        }
    }

    private void copyField(String propertyName, Schema sourceSchema, Schema targetSchema, Object source, Object target) {
        Property sourceProperty = sourceSchema.getProperties()
                                              .get(propertyName);
        Property targetProperty = targetSchema.getProperties()
                                              .get(propertyName);

        if (sourceProperty == null || targetProperty == null
                || !sourceProperty.getType().equals(targetProperty.getType())) {
            throw new RuntimeException("Specified property " + propertyName
                    + " doesn't exist or is not consistent");
        }

        try {
            Object sourceValue = sourceProperty.getGetterMethod()
                                               .invoke(source);
            if (sourceValue == null) {
                return;
            }
            if (sourceProperty.getType().isPrimitive()) {
                if (sourceValue instanceof Number) {
                    if (((Number) sourceValue).doubleValue() == 0.0) {
                        return;
                    }
                }
                if (sourceValue instanceof Boolean) {
                    if (((Boolean) sourceValue).equals(Boolean.FALSE)) {
                        return;
                    }
                }
            }
            targetProperty.getSetterMethod().invoke(target, sourceValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
