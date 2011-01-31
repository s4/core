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
import io.s4.util.GsonUtil;

import java.lang.reflect.Type;
import java.util.List;
import java.util.UUID;

import com.google.gson.InstanceCreator;

abstract public class Request {

    protected RInfo rinfo = nullRInfo;

    public final static RInfo nullRInfo = new NullRInfo();

    /**
     * Requester/Return information
     */
    abstract public static class RInfo {

        private long id = 0;

        /**
         * Identity of request. This is typically specified by the requester.
         */
        public long getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        private String stream;

        /**
         * Stream name on which response should be sent.
         * 
         * @return stream name.
         */
        public String getStream() {
            return stream;
        }

        public void setStream(String stream) {
            this.stream = stream;
        }

        private int partition;

        /**
         * Partition Id from which this request originated. This may be used to
         * return a response to the same partition.
         * 
         * @return
         */
        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        // Tell Gson how to instantiate one of these: create a ClientRInfo
        static {
            InstanceCreator<RInfo> creator = new InstanceCreator<RInfo>() {
                public io.s4.message.Request.RInfo createInstance(Type type) {
                    return new io.s4.message.Request.ClientRInfo();
                }
            };

            GsonUtil.registerTypeAdapter(RInfo.class, creator);
        }
        
    }

    public static class ClientRInfo extends RInfo {
        private UUID requesterUUID = null;

        /**
         * Identity of requesting client. This is used to send the response back
         * to the client.
         * 
         * @return UUID of the client from which the request originated.
         */
        public UUID getRequesterUUID() {
            return requesterUUID;
        }

        public void setRequesterUUID(UUID requesterUUID) {
            this.requesterUUID = requesterUUID;
        }

        public String toString() {
            return "(id:" + getId() + " requester:" + getRequesterUUID()
                    + " partition:" + getPartition() + " stream:" + getStream()
                    + ")";
        }
    }

    public static class PERInfo extends RInfo {
        private String requesterKey = null;

        /**
         * Identity of requesting PE. This is used to route the response back to
         * the originating PE.
         * 
         * @return key value of the PE from which the request originated.
         */
        public String getRequesterKey() {
            return requesterKey;
        }

        public void setRequesterKey(String requesterKey) {
            this.requesterKey = requesterKey;
        }

        public String toString() {
            return "(id:" + getId() + " requester:" + getRequesterKey()
                    + " partition:" + getPartition() + " stream:" + getStream()
                    + ")";
        }
    }

    public static class NullRInfo extends RInfo {
        public NullRInfo() {
            super.stream = "@null";
            super.partition = -1;
        }
    }

    /**
     * Query metainformation.
     * 
     * @return Info representing origin of request.
     */
    public RInfo getRInfo() {
        return rinfo;
    }

    /**
     * Query metainformation.
     */
    public void setRInfo(RInfo rinfo) {
        this.rinfo = rinfo;
    }

    /**
     * Partition itself. This is used by the default partitioner.
     * 
     * @param h
     *            hasher
     * @param delim
     *            delimiter used to concatenate compound key values
     * @param partCount
     *            number of partitions
     * @return list of compound keys: one event may have to be sent to multiple
     *         nodes.
     */
    abstract public List<CompoundKeyInfo> partition(Hasher h, String delim,
                                                    int partCount);
}