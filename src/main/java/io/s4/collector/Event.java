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

import io.s4.dispatcher.Dispatcher;
import io.s4.dispatcher.partitioner.CompoundKeyInfo;
import io.s4.dispatcher.partitioner.KeyInfo;
import io.s4.dispatcher.partitioner.KeyInfo.KeyPathElement;
import io.s4.dispatcher.partitioner.KeyInfo.KeyPathElementIndex;
import io.s4.dispatcher.partitioner.KeyInfo.KeyPathElementName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Event extends EventRecord {
    private String eventName;
    private long timestamp;
    private List<CompoundKeyInfo> compoundKeys = new ArrayList<CompoundKeyInfo>();
    private boolean debug = false;
    public static final String EVENT_NAME_KEY = "S4__eventName";
    public static final String TIMESTAMP_KEY = "S4__timestamp";

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public Event(Map<String, Object> passedEventData) {
        super(passedEventData);

        eventName = this.get(EVENT_NAME_KEY, "unknown");
        timestamp = this.get(TIMESTAMP_KEY, -1L);

        List<EventRecord> plainCompoundKeyList = null;
        if ((plainCompoundKeyList = get(Dispatcher.PARTITION_INFO_KEY,
                                        EMPTY_LIST)) != EMPTY_LIST) {
            for (EventRecord plainCompoundKeyInfo : plainCompoundKeyList) {
                CompoundKeyInfo compoundKeyInfo = new CompoundKeyInfo();
                compoundKeyInfo.setCompoundValue(plainCompoundKeyInfo.get("compoundValue",
                                                                          (String) null));
                compoundKeyInfo.setCompoundKey(plainCompoundKeyInfo.get("compoundKey",
                                                                        (String) null));
                compoundKeys.add(compoundKeyInfo);
                for (EventRecord plainKeyInfo : plainCompoundKeyInfo.get("keyInfoList",
                                                                         EMPTY_LIST)) {
                    KeyInfo keyInfo = new KeyInfo();
                    for (EventRecord plainKeyPathElement : plainKeyInfo.get("keyPathElementList",
                                                                            EMPTY_LIST)) {
                        String keyName = plainKeyPathElement.get("keyName",
                                                                 (String) null);
                        Integer index = plainKeyPathElement.get("index",
                                                                (Integer) null);

                        if (keyName != null) {
                            keyInfo.addElementToPath(keyName);
                        } else if (index != null) {
                            keyInfo.addElementToPath(index);
                        }
                    }
                    compoundKeyInfo.addKeyInfo(keyInfo);
                }
            }
        }
        if (debug) {
            for (CompoundKeyInfo compoundKeyInfo : compoundKeys) {
                System.out.println("CompoundKey: "
                        + compoundKeyInfo.getCompoundValue());
                for (KeyInfo keyInfo : compoundKeyInfo.getKeyInfoList()) {
                    String keyPath = "";
                    for (KeyPathElement keyPathElement : keyInfo.getKeyPath()) {
                        if (keyPathElement instanceof KeyPathElementIndex) {
                            keyPath += "["
                                    + ((KeyPathElementIndex) keyPathElement).getIndex()
                                    + "]";
                        } else {
                            if (keyPath.length() > 0) {
                                keyPath += "/";
                            }
                            keyPath += ((KeyPathElementName) keyPathElement).getKeyName();
                        }
                    }
                    System.out.println("   " + keyPath);
                }
            }
        }
    }

    public List<CompoundKeyInfo> getCompoundKeys() {
        return compoundKeys;
    }

    public String getEventName() {
        return eventName;
    }

    public long getTimeStamp() {
        return timestamp;
    }

    public List<Map<String, Object>> getCompoundKeyList() {
        return get(Dispatcher.PARTITION_INFO_KEY,
                   new ArrayList<Map<String, Object>>());
    }
}
