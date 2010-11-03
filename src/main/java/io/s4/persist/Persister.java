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
package io.s4.persist;

import java.util.Map;
import java.util.Set;

/**
 * Defines an interface to a collection of key/value pairs, each of which has a
 * time-to-live property.
 **/
public interface Persister {
    /**
     * Returns the number of <code>setAsynch</code>-initiated operations that
     * are pending or in-progress.
     * 
     * @return the number of pending set operations
     **/
    int getQueueSize();

    /**
     * Returns the number of <code>set</code> or <code>setAsynch</code> calls
     * made to this object.
     * 
     * @return the number of <code>set</code> or <code>setAsynch</code> calls
     *         made to this object.
     **/
    int getPersistCount();

    /**
     * Returns the number of key/value pairs in this Persister object.
     * <p>
     * If the underlying technology (e.g., Memcached) does not support this
     * operation, then the implementation may return 0 or a
     * <code>java.lang.UnsupportedOperationException</code>.
     * <p>
     * This number may include expired entries, depending on the implementation.
     * Some implementations clean up expired entries only when the value is
     * requested or at certain time intervals (or not at all).
     * 
     * @return the number of entries in this Persister
     **/
    int getCacheEntryCount();

    /**
     * Set a value for a key without waiting for the operation to complete.
     * <p>
     * This method is useful in the case where the underlying implementation
     * uses a network but the caller should not be delayed by the network
     * operation.
     * <p>
     * Because the set operation is not neccessarily complete when the method
     * returns, a subsequent <code>get</code> call may return stale data.
     * <p>
     * <code>period</code> specifies the time-to-live for this key/value pair.
     * For calls to <code>get</code> or <code>getObject</code> after the
     * specified period, the Persister object will return null.
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     * @param period
     *            the maximum interval of time during which this key/value pair
     *            is valid. Also know as time-to-live. -1 indicates an infinite
     *            time-to-live.
     **/
    void setAsynch(String key, Object value, int period);

    /**
     * Set a value for a key.
     * <p>
     * <code>period</code> specifies the time-to-live for this key/value pair.
     * For calls to <code>get</code> or <code>getObject</code> after the
     * specified period, the Persister object will return null.
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     * @param period
     *            the maximum interval of time during which this key/value pair
     *            is valid. Also know as time-to-live. -1 indicates an infinite
     *            time-to-live.
     **/
    void set(String key, Object value, int period) throws InterruptedException;

    /**
     * Get the value associated with a specified key.
     * 
     * If the period (aka time-to-live) for this key/value pair is expired, the
     * <code>get</code> will return null.
     * 
     * @param key
     *            the key
     **/
    Object get(String key) throws InterruptedException;

    /**
     * Get the values associated with a list of keys.
     * <p>
     * <code>getBulk</code> returns a <code>Map</code> containing an entry for
     * each key/value pair. The map contains an entry for a specified key only
     * if that key exists in the Persister and the associated key/value pair is
     * not yet expired.
     * 
     * @param keys
     *            a list of keys
     **/
    Map<String, Object> getBulk(String[] keys) throws InterruptedException;

    /**
     * This is a method to help support some implementations whose underlying
     * technology encodes the value associated with a key. In some cases, the
     * value may have been stored by a client that uses a form of encoding not
     * supported by the Persister's implementation. In that case, one might want
     * the raw, yet-to-be-decoded value associated with the key.
     * <p>
     * <code>getObject</code> retrieves the raw, unencoded value associated with
     * a key. In all other respects, it's the same as {@link Persister#get}.
     * <p>
     * It's likely you will never need to call this method.
     * 
     * If the period (aka time-to-live) for this key/value pair is expired, the
     * <code>get</code> will return null.
     **/
    Object getObject(String key) throws InterruptedException;

    /**
     * As with {@link Persister#getObject}, this is a method to help support
     * some implementations whose underlying technology encodes result.
     * <p>
     * It's likely you will never need to call this method.
     * <p>
     * In all other respects, it is the same as {@link Persister#getBulk}.
     **/
    Map<String, Object> getBulkObjects(String[] keys)
            throws InterruptedException;

    /**
     * Removes the entry for the specified key from the Persister object.
     * 
     * @param key
     *            the key of the entry to be removed
     **/
    void remove(String key) throws InterruptedException;

    /**
     * Forces the initiation of the process that cleans up expired entries.
     * <p>
     * Normally, you would not call this method. The implementation takes care
     * of the timing of the cleanup process.
     * <p>
     * In some implementations, this method may be a no-op.
     * 
     **/
    int cleanOutGarbage() throws InterruptedException;

    /**
     * Returns a <code>Set</code> of the keys contained in this Persister
     * object.
     * <p>
     * The <code>Set</code> may contain keys for expired entries, depending on
     * how often the underlying implementation cleans up expired entries.
     * <p>
     * The underlying technology may not support this operation. As a result,
     * some implementations return an empty set.
     * 
     * @return a <code>Set</code> of the keys contained in this Persister
     *         object.
     **/
    Set<String> keySet();
}
