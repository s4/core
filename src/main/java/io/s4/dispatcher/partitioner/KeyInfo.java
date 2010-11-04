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
import java.util.List;
import java.util.ArrayList;

public class KeyInfo implements Serializable {
    List<KeyPathElement> keyPath = new ArrayList<KeyPathElement>();
    String value;

    public void setValue(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public void addElementToPath(String keyName) {
        keyPath.add(new KeyPathElementName(keyName));
    }

    public void addElementToPath(int index) {
        keyPath.add(new KeyPathElementIndex(index));
    }

    private void addElementToPath(KeyPathElement keyPathElement) {
        keyPath.add(keyPathElement);
    }

    public List<KeyPathElement> getKeyPath() {
        return keyPath;
    }

    public static class KeyPathElement implements Serializable {
        public enum PathElementType {
            KEY_NAME, INDEX
        }

        PathElementType pathElementType;

        public PathElementType getPathElementType() {
            return pathElementType;
        }

    }

    public static class KeyPathElementName extends KeyPathElement {
        String keyName;

        public KeyPathElementName() {

        }

        public KeyPathElementName(String keyName) {
            pathElementType = PathElementType.KEY_NAME;
            this.keyName = keyName;
        }

        public String getKeyName() {
            return keyName;
        }
    }

    public static class KeyPathElementIndex extends KeyPathElement {
        int index;

        public KeyPathElementIndex() {

        }

        public KeyPathElementIndex(int index) {
            pathElementType = PathElementType.INDEX;
            this.index = index;
        }

        public int getIndex() {
            return index;
        }
    }

    public KeyInfo copy() {
        KeyInfo newKeyInfo = new KeyInfo();
        for (KeyPathElement element : keyPath) {
            newKeyInfo.addElementToPath(element);
        }
        return newKeyInfo;
    }

    public String toString() {
        return toString(false);
    }

    public String toString(boolean showFull) {
        StringBuffer sb = new StringBuffer();
        for (KeyPathElement element : keyPath) {
            if (element.getPathElementType() == KeyPathElement.PathElementType.KEY_NAME) {
                if (sb.length() > 0) {
                    sb.append("/");
                }
                sb.append(((KeyPathElementName) element).getKeyName());
            } else if (showFull) {
                sb.append("[")
                  .append(((KeyPathElementIndex) element).getIndex())
                  .append("]");
            }
        }
        return sb.toString();
    }
}
