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

package io.s4.util;

import io.s4.collector.EventWrapper;
import io.s4.processor.PEContainer;
import io.s4.schema.Schema;
import io.s4.schema.SchemaContainer;
import io.s4.schema.Schema.Property;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class EventClock extends DrivenClock {

    private static Logger logger = Logger.getLogger(PEContainer.class);
    Map<String, String> eventClockStreamsMap;
    SchemaContainer schemaContainer;

    public void init() {
        eventClockStreamsMap = new HashMap<String, String>();
        schemaContainer = new SchemaContainer();
    }

    public void update(EventWrapper eventWrapper) {
        long eventTime = -1;
        String streamName = eventWrapper.getStreamName();
        if (eventClockStreamsMap.containsKey(streamName)) {
            String fieldName = eventClockStreamsMap.get(streamName);
            Object event = eventWrapper.getEvent();
            Schema schema = schemaContainer.getSchema(event.getClass());
            if (fieldName != null) {
                Property property = schema.getProperties().get(fieldName);
                if (property != null
                        && (property.getType().equals(Long.TYPE) || property
                                .getType().equals(Long.class))) {
                    try {
                        eventTime = (Long) property.getGetterMethod().invoke(
                                event);
                        updateTime(eventTime);
                        logger.debug("Updating Event Clock time "
                                + eventTime);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    public void addEventClockStream(String streamName, String fieldName) {
        if (eventClockStreamsMap.containsKey(streamName)) {
            if (!eventClockStreamsMap.get(streamName).equals(fieldName)) {
                // we can add an runtime exception over error messages for
                // making debugging easy
                logger.error("Stream " + streamName
                        + " already has a timestamp field defined "
                        + eventClockStreamsMap.get(streamName));
                logger.error("Stream " + streamName
                        + " is updating the timestamp field to " + fieldName);
            }
        } else {
            eventClockStreamsMap.put(streamName, fieldName);
            System.out.println("adding stream " + streamName + " with field " + fieldName );
        }
        eventClockStreamsMap.put(streamName, fieldName);
    }

}
