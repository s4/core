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

import java.util.List;
import java.util.UUID;

abstract public class Request {

    protected Info rinfo;

    abstract public static class Info {
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

    }

    public static class ClientInfo extends Info {
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

    public static class PEInfo extends Info {
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

    /**
     * Query metainformation.
     * 
     * @return Info representing origin of request.
     */
    public Info getInfo() {
        return rinfo;
    }

    abstract public List<CompoundKeyInfo> partition(Hasher h, String delim,
                                                    int partCount);
}