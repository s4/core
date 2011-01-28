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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Response {

    private Map<String, Object> result;

    private Map<String, String> exception;

    private Request request;

    public Response(Map<String, Object> result, Request request) {
        this.result = result;
        this.request = request;
    }

    public Response(Map<String, Object> result, Map<String, String> exception,
            Request request) {
        this.result = result;
        this.exception = exception;
        this.request = request;
    }

    public Response() {
        result = null;
        exception = null;
        request = null;
    }

    /**
     * Result of a request.
     * 
     * @return map from query strings o corresponding values.
     */
    public Map<String, Object> getResult() {
        return result;
    }

    public Map<String, String> getException() {
        return exception;
    }

    public Request getRequest() {
        return request;
    }

    public Request.RInfo getRInfo() {
        return (request != null ? request.getRInfo() : null);
    }

    public String toString() {
        return "[" + result + "] (" + request + ")";
    }

    public List<CompoundKeyInfo> partition(int partCount) {
        // partition id is available from the request info object

        int p = this.getRInfo().getPartition();
        List<CompoundKeyInfo> partitionInfoList = null;

        if (p >= 0 && p < partCount) {
            CompoundKeyInfo partitionInfo = new CompoundKeyInfo();
            partitionInfo.setPartitionId(p);

            partitionInfoList = new ArrayList<CompoundKeyInfo>();
            partitionInfoList.add(partitionInfo);
        }

        return partitionInfoList;
    }

}