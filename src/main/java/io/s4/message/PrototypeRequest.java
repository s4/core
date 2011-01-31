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
import io.s4.processor.PrototypeWrapper;
import io.s4.util.MethodInvoker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * A request for a value from the prototype of PEs.
 */
public class PrototypeRequest extends Request {

    private final List<String> query;

    public PrototypeRequest(List<String> query, RInfo info) {
        this.query = query;
        this.rinfo = info;
    }

    public PrototypeRequest(List<String> query) {
        this.query = query;
        this.rinfo = null;
    }

    public PrototypeRequest() {
        this.query = Collections.<String> emptyList();
        this.rinfo = null;
    }

    public String toString() {
        return "PROTOTYPE: query=[" + query + "] info=[" + rinfo + "]";
    }

    /**
     * List of queries to execute.
     * 
     * @return list of queries
     */
    public List<String> getQuery() {
        return query;
    }

    /**
     * Evaluate Request on a particular PE Prototype.
     * 
     * @param pe
     *            prototype
     * @return Response object.
     */
    public Response evaluate(PrototypeWrapper pw) {

        HashMap<String, Object> results = new HashMap<String, Object>();
        HashMap<String, String> exceptions = new HashMap<String, String>();

        for (String q : query) {
            if (q.startsWith("$")) {
                // requests for getters should be of the form $fieldA.
                // Responds with pe.getFieldA()
                try {
                    Object res = MethodInvoker.invokeGetter(pw, q.substring(1));
                    results.put(q, res);

                } catch (Exception e) {
                    exceptions.put(q, e.toString());
                }

            } else if (q.equalsIgnoreCase("count")) {
                // Some aggregate operators
                results.put(q, pw.getPECount());

            } else {
                exceptions.put(q, "Query Parse Error");
            }
        }

        return new Response(results, exceptions, this);
    }

    public List<CompoundKeyInfo> partition(Hasher h, String delim, int partCount) {
        // send to all partitions
        List<CompoundKeyInfo> partitionInfoList = new ArrayList<CompoundKeyInfo>();

        for (int i = 0; i < partCount; ++i) {
            CompoundKeyInfo partitionInfo = new CompoundKeyInfo();
            partitionInfo.setPartitionId(i);
            partitionInfoList.add(partitionInfo);
        }

        return partitionInfoList;
    }
}
