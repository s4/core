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
import io.s4.schema.Schema;
import io.s4.schema.Schema.Property;
import io.s4.util.SlotUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

public abstract class AbstractWindowingPE extends AbstractPE {
    private String slotClassName;
    private int slotSize = 3600; // default one hour
    private int windowSize = slotSize * 24; // default, 24 hours
    private Map<String, String> timestampFields;

    private long lastTimestamp = -1;
    private Map<Long, Slot> slots;
    private SlotUtils slotUtils;
    private Class slotClass;

    public void setSlotClassName(String slotClassName) {
        this.slotClassName = slotClassName;
    }

    public void setSlotSize(int slotSize) {
        this.slotSize = slotSize;
    }

    public int getSlotSize() {
        return slotSize;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    public void setTimestampFields(String[] timestampFieldsArray) {
        timestampFields = new HashMap<String, String>();
        for (String timeStampFieldInfo : timestampFieldsArray) {
            StringTokenizer st = new StringTokenizer(timeStampFieldInfo);
            timestampFields.put(st.nextToken(), st.nextToken());
        }
    }

    private OverloadDispatcherSlot overloadDispatcher;

    public AbstractWindowingPE() {
    }

    public void init() {
        // this reference will be shared amongst all instances of the pe
        slotUtils = new SlotUtils(slotSize);

        try {
            slotClass = Class.forName(slotClassName);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(cnfe);
        }

        OverloadDispatcherGenerator oldg = new OverloadDispatcherGenerator(slotClass,
                                                                           true);
        Class<?> overloadDispatcherClass = oldg.generate();

        try {
            overloadDispatcher = (OverloadDispatcherSlot) overloadDispatcherClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void processEvent(Object event) {
        long currentTime = System.currentTimeMillis();
        long maybeCurrentTime = -1;
        if (timestampFields != null) {
            Schema schema = schemaContainer.getSchema(event.getClass());
            String fieldName = timestampFields.get(getStreamName());
            if (fieldName != null) {
                Property property = schema.getProperties().get(fieldName);
                if (property != null
                        && (property.getType().equals(Long.TYPE) || property.getType()
                                                                            .equals(Long.class))) {
                    try {
                        maybeCurrentTime = (Long) property.getGetterMethod()
                                                          .invoke(event);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        if (maybeCurrentTime > -1) {
            currentTime = maybeCurrentTime;
            lastTimestamp = currentTime;
        }

        long slotTime = slotUtils.getSlotAtTime(currentTime / 1000); // convert
                                                                     // to
                                                                     // seconds

        if (slots == null) {
            slots = Collections.synchronizedMap(new HashMap<Long, Slot>());
        }

        Slot slot = slots.get(slotTime);
        if (slot == null) {
            try {
                slot = (Slot) slotClass.newInstance();
            } catch (IllegalAccessException iae) {
                throw new RuntimeException(iae);
            } catch (InstantiationException ie) {
                throw new RuntimeException(ie);
            }
            slots.put(slotTime, slot);
        }

        overloadDispatcher.dispatch(slot, event, slotTime, this);
    }

    public Map<Long, Slot> getSlots() {
        pruneSlots(getCurrentTime() / 1000);

        return Collections.unmodifiableMap(slots);
    }

    private void pruneSlots(long time) {
        HashSet<Long> keys = new HashSet<Long>();

        synchronized (slots) {
            for (Long key : slots.keySet()) {
                keys.add(key);
            }
        }

        for (Long key : keys) {
            if (slotUtils.isOutsideWindow(key, windowSize, time)) {
                slots.remove(key);
            }
        }
    }

    public boolean isCurrentSlot(long slotTime) {
        long currentSlot = slotUtils.getSlotAtTime(getCurrentTime() / 1000);
        if (currentSlot == slotTime) {
            return true;
        }
        return false;
    }

    public Long getSlotAtOffset(int offset) {
        return slotUtils.getSlot(offset, getCurrentTime() / 1000);
    }

    public static interface Slot {
        // public void processEvent(Object event, long slotTime,
        // AbstractWindowingPE pe);
    }

}
