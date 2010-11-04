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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;

public class EventRecord implements Map<String, Object> {

    public static EventRecord EMPTY_RECORD = new EventRecord(new HashMap<String, Object>());
    public static List<EventRecord> EMPTY_LIST = Collections.unmodifiableList(new ArrayList<EventRecord>());

    private Map<String, Object> eventData = new HashMap<String, Object>();
    private Map<String, Object> additionalData = new HashMap<String, Object>();

    public EventRecord(Map<String, Object> passedEventData) {
        this(passedEventData, true);
    }

    private EventRecord(Map<String, Object> passedEventData,
            boolean processEventData) {
        if (processEventData) {
            eventData = processMap(passedEventData, true);
        } else {
            eventData = passedEventData;
        }
    }

    private Map<String, Object> processMap(Map<String, Object> inputMap) {
        return processMap(inputMap, false);
    }

    private Map<String, Object> processMap(Map<String, Object> inputMap, boolean returnRaw) {
        Map<String, Object> eventData = new HashMap<String, Object>();
        for (String key : inputMap.keySet()) {
            Object value = inputMap.get(key);
            if (value instanceof Map<?, ?>) {
                eventData.put(key, processMap((Map<String, Object>) value));
            } else if (value instanceof List<?>) {
                eventData.put(key,
                              processList((List<Map<String, Object>>) value));
            } else {
                eventData.put(key, value);
            }
        }
        if (returnRaw)
            return eventData;
        return new EventRecord(eventData, false);
    }

    private List<Map<String, Object>> processList(List<Map<String, Object>> inputList) {
        List<Map<String, Object>> eventList = new ArrayList<Map<String, Object>>();
        for (Map<String, Object> inputMap : inputList) {
            eventList.add(processMap(inputMap));
        }
        return Collections.unmodifiableList(eventList);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key) {
        return eventData.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return eventData.containsValue(value);
    }

    @Override
    public Set<java.util.Map.Entry<String, Object>> entrySet() {
        return eventData.entrySet();
    }

    @Override
    public Object get(Object key) {
        return eventData.get(key);
    }

    public <T> T get(String key, T defaultValue) {
        return get(key, defaultValue, eventData);
    }

    private <T> T get(String key, T defaultValue, Map<String, Object> map) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        return (T) value;
    }

    @Override
    public boolean isEmpty() {
        return eventData.isEmpty();
    }

    @Override
    public Set<String> keySet() {
        return eventData.keySet();
    }

    @Override
    public Object put(String key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return eventData.size();
    }

    @Override
    public Collection<Object> values() {
        return eventData.values();
    }

    public void setAdditionalProperty(String key, Object value) {
        additionalData.put(key, value);
    }

    public <T> T getAdditionalProperty(String key, T defaultValue) {
        return get(key, defaultValue, additionalData);
    }

    public void removeAdditionalProperty(String key) {
        additionalData.remove(key);
    }

    public Map<String, Object> getMutableMap() {
        return getMutableMap(this.eventData);
    }

    public Map<String, Object> getMutableMap(Map<String, Object> recordData) {
        Map<String, Object> mutableData = new HashMap<String, Object>();
        for (String key : recordData.keySet()) {
            Object value = recordData.get(key);
            if (value instanceof Map<?, ?>) {
                mutableData.put(key, getMutableMap((Map<String, Object>) value));
            } else if (value instanceof List<?>) {
                mutableData.put(key,
                                getMutableList((List<Map<String, Object>>) value));
            } else {
                mutableData.put(key, value);
            }
        }
        return mutableData;
    }

    public List<Map<String, Object>> getMutableList(List<Map<String, Object>> recordList) {
        List<Map<String, Object>> mutableList = new ArrayList<Map<String, Object>>();
        for (Map<String, Object> recordData : recordList) {
            mutableList.add(getMutableMap(recordData));
        }
        return mutableList;
    }

}
