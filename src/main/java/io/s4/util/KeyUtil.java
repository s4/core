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
package io.s4.util;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * from http://github.com/dustin/java-memcached-client/blob/master/src/main/java/net/spy/memcached/KeyUtil.java
 */
/**
 * Utilities for processing key values.
 */
public class KeyUtil {

    /**
     * Get the bytes for a key.
     * 
     * @param k
     *            the key
     * @return the bytes
     */
    public static byte[] getKeyBytes(String k) {
        try {
            return k.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the keys in byte form for all of the string keys.
     * 
     * @param keys
     *            a collection of keys
     * @return return a collection of the byte representations of keys
     */
    public static Collection<byte[]> getKeyBytes(Collection<String> keys) {
        Collection<byte[]> rv = new ArrayList<byte[]>(keys.size());
        for (String s : keys) {
            rv.add(getKeyBytes(s));
        }
        return rv;
    }
}