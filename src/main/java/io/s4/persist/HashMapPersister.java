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

import io.s4.util.clock.Clock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class HashMapPersister implements Persister {
    private volatile int persistCount = 0;
    private boolean selfClean = false;
    private int cleanWaitTime = 40; // 40 seconds by default
    private String loggerName = "s4";
    Map<String, CacheEntry> cache;
    Clock s4Clock;

    private int startCapacity = 5000;

    public void setStartCapacity(int startCapacity) {
        this.startCapacity = startCapacity;
    }

    public int getStartCapacity() {
        return startCapacity;
    }

    public void setSelfClean(boolean selfClean) {
        this.selfClean = selfClean;
    }

    public void setCleanWaitTime(int cleanWaitTime) {
        this.cleanWaitTime = cleanWaitTime;
    }

    public void setLoggerName(String loggerName) {
        this.loggerName = loggerName;
    }

    public HashMapPersister(Clock s4Clock) {
        this.s4Clock = s4Clock;
    }
    
    public void setS4Clock(Clock s4Clock) {
        this.s4Clock = s4Clock;
    }

    public void init() {
        cache = Collections.synchronizedMap(new HashMap<String, CacheEntry>(this.getStartCapacity()));

        if (selfClean) {
            Runnable r = new Runnable() {
                public void run() {
                    while (!Thread.interrupted()) {
                        int cleanCount = HashMapPersister.this.cleanOutGarbage();
                        Logger.getLogger(loggerName).info("Cleaned out "
                                + cleanCount + " entries; Persister has "
                                + cache.size() + " entries");
                        try {
                            Thread.sleep(cleanWaitTime * 1000);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            };
            Thread t = new Thread(r);
            t.start();
            t.setPriority(Thread.MIN_PRIORITY);
        }
    }

    public int getQueueSize() {
        return 0;
    }

    public int getPersistCount() {
        return persistCount;
    }

    public int getCacheEntryCount() {
        return cache.size();
    }

    public void setAsynch(String key, Object value, int period) {
        // there really is no asynch for the local cache
        set(key, value, period);
    }

    public void set(String key, Object value, int period) {
        synchronized (this) {
            persistCount++;
        }

        CacheEntry ce = new CacheEntry();
        ce.value = value;
        ce.period = period;
        ce.addTime = s4Clock.getCurrentTime();
        cache.put(key, ce);
    }

    public Object get(String key) {
        CacheEntry ce = cache.get(key);
        if (ce == null) {
            return null;
        }

        if (ce.isExpired()) {
            return null;
        }

        return ce.value;
    }

    public Map<String, Object> getBulk(String[] keys) {
        HashMap map = new HashMap<String, Object>();
        for (String key : keys) {
            Object value = get(key);
            if (value != null) {
                map.put(key, value);
            }
        }
        return map;
    }

    public Object getObject(String key) {
        return get(key);
    }

    public Map<String, Object> getBulkObjects(String[] keys) {
        return getBulk(keys);
    }

    public void remove(String key) {
        cache.remove(key);
    }

    public int cleanOutGarbage() {
        int count = 0;
        List<String> keyList;
        synchronized (cache) {
            keyList = new ArrayList<String>(cache.size());
            for (String key : cache.keySet()) {
                keyList.add(key);
            }
        }

        for (String key : keyList) {
            CacheEntry ce = cache.get(key);
            if (ce != null && ce.isExpired()) {
                count++;
                cache.remove(key);
            }
        }
        return count;
    }

    public Set<String> keySet() {
        Set<String> keys = new HashSet<String>();
        synchronized (cache) {
            for (String key : cache.keySet()) {
                keys.add(key);
            }
        }
        return keys;
    }

    public class CacheEntry {
        Object value;
        long addTime;
        int period;

        public boolean isExpired() {
            if (period > 0) {
                if ((addTime + (1000 * (long) period)) <= s4Clock.getCurrentTime()) {
                    return true;
                }
            }
            return false;
        }
    }
}
