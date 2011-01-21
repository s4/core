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
package io.s4.dispatcher.partitioner;

import io.s4.schema.Schema;
import io.s4.schema.Schema.Property;
import io.s4.schema.SchemaContainer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.springframework.util.StringUtils;

public class DefaultPartitioner implements Partitioner, VariableKeyPartitioner {
    private List<List<String>> keyNameTuple = new ArrayList<List<String>>();
    private boolean debug = false;
    private Hasher hasher;
    private Set<String> streamNameSet;
    private String delimiter = ":";
    private boolean fastPath = false;

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public void setHashKey(String[] simpleKeyStrings) {
        for (String simpleKeyAsString : simpleKeyStrings) {
            List<String> keyNameElements = new ArrayList<String>();
            StringTokenizer st = new StringTokenizer(simpleKeyAsString, "/");
            while (st.hasMoreTokens()) {
                keyNameElements.add(st.nextToken());
            }
            keyNameTuple.add(keyNameElements);
        }
    }

    public void setStreamNames(String[] streamNames) {
        streamNameSet = new HashSet<String>(streamNames.length);
        for (String eventType : streamNames) {
            streamNameSet.add(eventType);
        }
    }

    public void setHasher(Hasher hasher) {
        this.hasher = hasher;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    private SchemaContainer schemaContainer = new SchemaContainer();

    public List<CompoundKeyInfo> partition(String streamName, Object event,
                                           int partitionCount) {
        return partition(streamName, keyNameTuple, event, partitionCount);
    }

    public List<CompoundKeyInfo> partition(String streamName,
                                           List<List<String>> compoundKeyNames,
                                           Object event, int partitionCount) {

        if (streamName != null && streamNameSet != null
                && !streamNameSet.contains(streamName)) {
            return null;
        }

        // Some event types that need special handling
        if (event instanceof io.s4.message.Request) {
            // construct key from request's target
            io.s4.message.Request r = (io.s4.message.Request) event;
            return r.partition(hasher, delimiter, partitionCount);

        } else if (event instanceof io.s4.message.Response) {
            // partition id is encoded in Response, so use it directly.
            io.s4.message.Response r = (io.s4.message.Response) event;
            return r.partition(partitionCount);

        } else if (compoundKeyNames == null) {
            // if compoundKeyNames is null, then assign to a random partition.
            return partitionRandom(partitionCount);
        }

        // have to compute key value and
        // partition based on hash of that value

        Schema schema = schemaContainer.getSchema(event.getClass());

        if (debug) {
            System.out.println(schema);
        }

        List<CompoundKeyInfo> partitionInfoList = new ArrayList<CompoundKeyInfo>();

        // fast path for single top-level key
        if (fastPath
                || (compoundKeyNames.size() == 1 && compoundKeyNames.get(0)
                                                                    .size() == 1)) {
            String simpleKeyName = compoundKeyNames.get(0).get(0);
            if (debug) {
                System.out.println("Using fast path!");
            }
            fastPath = true;
            KeyInfo keyInfo = new KeyInfo();
            Property property = schema.getProperties().get(simpleKeyName);
            if (property == null) {
                return null;
            }

            Object value = null;
            try {
                value = property.getGetterMethod().invoke(event);
            } catch (Exception e) {
                if (debug) {
                    e.printStackTrace();
                }
            }

            if (value == null) {
                if (debug) {
                    System.out.println("Fast path: Null value encountered");
                }
                return null;
            }
            keyInfo.addElementToPath(simpleKeyName);
            String stringValue = String.valueOf(value);
            keyInfo.setValue(stringValue);
            CompoundKeyInfo partitionInfo = new CompoundKeyInfo();
            partitionInfo.addKeyInfo(keyInfo);
            int partitionId = (int) (hasher.hash(stringValue) % partitionCount);
            partitionInfo.setPartitionId(partitionId);
            partitionInfo.setCompoundValue(stringValue);
            partitionInfoList.add(partitionInfo);
            if (debug) {
                System.out.printf("Value %s, partition id %d\n",
                                  stringValue,
                                  partitionInfo.getPartitionId());
            }
            return partitionInfoList;
        }

        List<List<KeyInfo>> valueLists = new ArrayList<List<KeyInfo>>();
        int maxSize = 0;

        for (List<String> simpleKeyPath : compoundKeyNames) {
            List<KeyInfo> keyInfoList = new ArrayList<KeyInfo>();
            KeyInfo keyInfo = new KeyInfo();
            keyInfoList = getKeyValues(event,
                                       schema,
                                       simpleKeyPath,
                                       0,
                                       keyInfoList,
                                       keyInfo);
            if (keyInfoList == null || keyInfoList.size() == 0) {
                if (debug) {
                    System.out.println("Null value encountered");
                }
                return null; // do no partitioning if any simple key's value
                             // resolves to null
            }
            valueLists.add(keyInfoList);
            maxSize = Math.max(maxSize, keyInfoList.size());

            if (debug) {
                printKeyInfoList(keyInfoList);
            }
        }

        for (int i = 0; i < maxSize; i++) {
            String compoundValue = "";
            CompoundKeyInfo partitionInfo = new CompoundKeyInfo();
            for (List<KeyInfo> keyInfoList : valueLists) {
                if (i < keyInfoList.size()) {
                    compoundValue += (compoundValue.length() > 0 ? delimiter
                            : "") + keyInfoList.get(i).getValue();
                    partitionInfo.addKeyInfo(keyInfoList.get(i));
                } else {
                    compoundValue += (compoundValue.length() > 0 ? delimiter
                            : "")
                            + keyInfoList.get(keyInfoList.size() - 1)
                                         .getValue();
                    partitionInfo.addKeyInfo(keyInfoList.get(keyInfoList.size() - 1));
                }
            }

            // get the partition id
            int partitionId = (int) (hasher.hash(compoundValue) % partitionCount);
            partitionInfo.setPartitionId(partitionId);
            partitionInfo.setCompoundValue(compoundValue);
            partitionInfoList.add(partitionInfo);
            if (debug) {
                System.out.printf("Value %s, partition id %d\n",
                                  compoundValue,
                                  partitionInfo.getPartitionId());
            }
        }

        return partitionInfoList;
    }

    // Assign to random partition
    private List<CompoundKeyInfo> partitionRandom(int partitionCount) {
        CompoundKeyInfo partitionInfo = new CompoundKeyInfo();

        // choose a random int from [0, partitionCount-1]
        int partitionId = (int) Math.min(partitionCount - 1,
                                         Math.floor(Math.random()
                                                 * partitionCount));

        partitionInfo.setPartitionId(partitionId);
        List<CompoundKeyInfo> partitionInfoList = new ArrayList<CompoundKeyInfo>();
        partitionInfoList.add(partitionInfo);

        return partitionInfoList;
    }

    private void printKeyInfoList(List<KeyInfo> keyInfoList) {
        for (KeyInfo aKeyInfo : keyInfoList) {
            System.out.printf("Path: %s; full path %s; value %s\n",
                              aKeyInfo.toString(),
                              aKeyInfo.toString(true),
                              aKeyInfo.getValue());
        }
    }

    private List<KeyInfo> getKeyValues(Object record, Schema schema,
                                       List<String> keyNameElements,
                                       int elementIndex,
                                       List<KeyInfo> keyInfoList,
                                       KeyInfo keyInfo) {
        String keyElement = keyNameElements.get(elementIndex);
        Property property = schema.getProperties().get(keyElement);
        if (property == null) {
            return null;
        }

        keyInfo.addElementToPath(keyElement);

        Object value = null;
        try {
            value = property.getGetterMethod().invoke(record);
        } catch (Exception e) {
            if (debug) {
                System.out.println("key element is " + keyElement);
                e.printStackTrace();
            }
        }

        if (value == null) {
            return null; // return a null KeyInfo list if we hit a null value
        }
        if (property.isList()) {
            List list = (List) value;
            // TODO: handle case where key does not include property of
            // component type
            Schema componentSchema = property.getComponentProperty()
                                             .getSchema();
            int listLength = list.size();
            for (int i = 0; i < listLength; i++) {
                Object listEntry = list.get(i);
                KeyInfo keyInfoForListEntry = keyInfo.copy();
                keyInfoForListEntry.addElementToPath(i);
                Object partialList = getKeyValues(listEntry,
                                                  componentSchema,
                                                  keyNameElements,
                                                  elementIndex + 1,
                                                  keyInfoList,
                                                  keyInfoForListEntry);
                if (partialList == null) {
                    return null;
                }
            }
        } else if (property.getSchema() != null) {
            return getKeyValues(value,
                                property.getSchema(),
                                keyNameElements,
                                elementIndex + 1,
                                keyInfoList,
                                keyInfo);
        } else {
            keyInfo.setValue(String.valueOf(value));
            keyInfoList.add(keyInfo);
        }

        return keyInfoList;
    }

    public static void main(String args[]) {
        DefaultPartitioner dp1 = new DefaultPartitioner();
        DefaultPartitioner dp2 = new DefaultPartitioner();
        dp1.setDebug(true);
        dp1.setHashKey(new String[] { "array1/val1", "array1/val2", "query" });
        dp1.setHasher(new DefaultHasher());

        dp2.setDebug(true);
        dp2.setHashKey(new String[] { "user" });
        dp2.setHasher(new DefaultHasher());

        Map<String, Object> event = new HashMap<String, Object>();
        event.put("user", "fred");
        event.put("query", "timex watch");
        List<Map<String, Object>> array1 = new ArrayList<Map<String, Object>>();
        Map<String, Object> element = new HashMap<String, Object>();
        element.put("val1", new Long(72));
        element.put("val2", new Long(11));
        array1.add(element);
        element = new HashMap<String, Object>();
        element.put("val1", new Long(21));
        element.put("val2", new Long(12));
        array1.add(element);
        event.put("array1", array1);

        dp1.partition("test", event, 4);
        System.out.println("------------");
        dp2.partition("test", event, 4);
        System.out.println("------------");
        event = new HashMap<String, Object>();

        event.put("query", "timex watch");
        array1 = new ArrayList<Map<String, Object>>();
        element = new HashMap<String, Object>();
        element.put("val1", new Long(72));
        element.put("val2", new Long(11));
        array1.add(element);
        element = new HashMap<String, Object>();

        element.put("val2", new Long(12));
        array1.add(element);
        event.put("array1", array1);

        dp1.partition("test", event, 4);
        System.out.println("------------");
        dp2.partition("test", event, 4);
    }
}
