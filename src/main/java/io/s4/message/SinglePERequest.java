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
package io.s4.message;

import io.s4.dispatcher.partitioner.CompoundKeyInfo;
import io.s4.dispatcher.partitioner.Hasher;
import io.s4.dispatcher.partitioner.KeyInfo;
import io.s4.processor.ProcessingElement;
import io.s4.util.MethodInvoker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.springframework.util.StringUtils;

/**
 * A request for a value from a particular PE.
 */
public class SinglePERequest extends Request {

    private final List<String> target;

    private final List<String> query;

    public SinglePERequest(List<String> target, List<String> query, Info info) {
        this.target = target;
        this.query = query;
        this.rinfo = info;
    }

    public SinglePERequest(List<String> target, List<String> query) {
        this.target = target;
        this.query = query;
        this.rinfo = null;
    }

    public SinglePERequest() {
        this.target = Collections.<String> emptyList();
        this.query = Collections.<String> emptyList();
        this.rinfo = null;
    }

    public String toString() {
        return "target:" + target + " query:" + query + " info:" + rinfo;
    }

    /**
     * Fields used to target a particular PE.
     * 
     * @return list of targeting values. Order matters.
     */
    public List<String> getTarget() {
        return target;
    }

    /**
     * List of field names that have to be read form target PE.
     * 
     * @return list of field name strings.
     */
    public List<String> getQuery() {
        return query;
    }

    /**
     * Evaluate Request on a particular PE.
     * 
     * @param pe
     * @return Response object.
     */
    public Response evaluate(ProcessingElement pe) {

        HashMap<String, Object> results = new HashMap<String, Object>();
        HashMap<String, String> exceptions = new HashMap<String, String>();

        for (String q : query) {
            // requests for getters should be of the form $field. Responds with
            // pe.getField()
            if (q.startsWith("$")) {
                try {
                    Object res = MethodInvoker.invokeGetter(pe, q.substring(1));
                    results.put(q, res);

                } catch (Exception e) {
                    exceptions.put(q, e.toString());
                }
            }
        }

        return new Response(results, exceptions, this);
    }

    public List<CompoundKeyInfo> partition(Hasher h, String delim, int partCount) {
        List<String> valueList = this.getTarget();
        if (valueList == null)
            return null;

        // First, build the key
        KeyInfo keyInfo = new KeyInfo();
        // special kay name to denote request
        keyInfo.addElementToPath("#req");

        // for value, concatenate list of values from Request's target field.
        String stringValue = StringUtils.collectionToDelimitedString(valueList,
                                                                     delim);
        keyInfo.setValue(stringValue);

        // partition id is derived form string value, as usual
        int partitionId = (int) (h.hash(stringValue) % partCount);

        CompoundKeyInfo partitionInfo = new CompoundKeyInfo();
        partitionInfo.addKeyInfo(keyInfo);
        partitionInfo.setCompoundValue(stringValue);
        partitionInfo.setPartitionId(partitionId);

        List<CompoundKeyInfo> partitionInfoList = new ArrayList<CompoundKeyInfo>();
        partitionInfoList.add(partitionInfo);

        return partitionInfoList;
    }

}
