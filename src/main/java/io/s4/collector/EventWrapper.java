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
package io.s4.collector;

import io.s4.dispatcher.partitioner.CompoundKeyInfo;

import java.util.ArrayList;
import java.util.List;

public class EventWrapper {
    private List<CompoundKeyInfo> compoundKeys = new ArrayList<CompoundKeyInfo>();
    private Object event;
    private String streamName;

    public List<CompoundKeyInfo> getCompoundKeys() {
        return compoundKeys;
    }

    public Object getEvent() {
        return event;
    }

    public String getStreamName() {
        return streamName;
    }

    public EventWrapper() {

    }

    public EventWrapper(String streamName, Object event,
            List<CompoundKeyInfo> compoundKeys) {
        this.streamName = streamName;
        this.event = event;
        this.compoundKeys = compoundKeys;
    }
}
