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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CompoundKeyInfo implements Serializable {
    List<KeyInfo> keyInfoList = new ArrayList<KeyInfo>();
    int partitionId = -1;
    String compoundValue;
    String compoundKey;

    public CompoundKeyInfo() {
    }

    public void addKeyInfo(KeyInfo keyInfo) {
        keyInfoList.add(keyInfo);
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public void setCompoundKey(String compoundKey) {
        this.compoundKey = compoundKey;
    }

    public void setCompoundValue(String compoundValue) {
        this.compoundValue = compoundValue;
    }

    public List<KeyInfo> getKeyInfoList() {
        return keyInfoList;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getCompoundKey() {
        if (compoundKey == null) {
            StringBuffer compoundKeyBuffer = new StringBuffer();
            for (KeyInfo keyInfo : this.getKeyInfoList()) {
                if (compoundKeyBuffer.length() > 0) {
                    compoundKeyBuffer.append(",");
                }
                compoundKeyBuffer.append(keyInfo.toString());
            }
            compoundKey = compoundKeyBuffer.toString();
        }
        return compoundKey;
    }

    public String getCompoundValue() {
        return this.compoundValue;
    }

    public String toString() {
        return "{" + getCompoundKey() + " = " + getCompoundValue() + "}:"
                + getPartitionId();
    }
}
