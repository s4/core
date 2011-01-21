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
import java.util.StringTokenizer;

public class EventWrapper {
    private List<CompoundKeyInfo> compoundKeys = null;
    private List<List<String>> compoundKeyNames = null;
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

    public List<List<String>> getCompoundKeyNames() {
        return compoundKeyNames;
    }

    public EventWrapper() {
        compoundKeys = new ArrayList<CompoundKeyInfo>();
    }

    public EventWrapper(String streamName, Object event,
            List<CompoundKeyInfo> compoundKeys) {
        this.streamName = streamName;
        this.event = event;
        this.compoundKeys = compoundKeys;
    }

    public EventWrapper(String streamName, String[] compoundKeyStrings,
            Object event) {
        this.streamName = streamName;
        this.event = event;

        if (compoundKeyStrings != null) {
            this.compoundKeyNames = new ArrayList<List<String>>(compoundKeyStrings.length);

            for (String keyAsString : compoundKeyStrings) {
                List<String> keyNameElements = new ArrayList<String>();
                StringTokenizer st = new StringTokenizer(keyAsString, "/");
                while (st.hasMoreTokens()) {
                    keyNameElements.add(st.nextToken());
                }
                compoundKeyNames.add(keyNameElements);
            }
        }
    }

    public String toString() {
        return "stream:" + getStreamName() + " keys:" + getCompoundKeys()
                + " keyNames:" + getCompoundKeyNames() + " event:" + getEvent();
    }
}
