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
package io.s4.logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class Log4jMonitor extends TimerTask implements Monitor {
    Map<String, Integer> metricMap = new ConcurrentHashMap<String, Integer>();
    private String loggerName = "s4";
    private int flushInterval = 600; // default is every 10 minutes

    private Timer timer = new Timer();
    private Map<String, Integer> defaultMap = new HashMap<String, Integer>();

    public void setLoggerName(String loggerName) {
        this.loggerName = loggerName;
    }

    public void setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
    }

    public void init() {
        if (flushInterval > 0) {
            timer.scheduleAtFixedRate(this,
                                      flushInterval * 1000,
                                      flushInterval * 1000);
        }
    }

    // TODO: this will be removed after changing above functions
    public void set(String metricName, int value) {
        metricMap.put(metricName, value);
    }

    public void flushStats() {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(loggerName);
        for (String key : metricMap.keySet()) {
            String message = key + " = " + metricMap.get(key);
            logger.info(message);
            metricMap.remove(key);
        }
        if (defaultMap != null) {
            for (String key : defaultMap.keySet()) {
                // TODO: need to be changed
                set(key, defaultMap.get(key));
            }
        }
    }

    public void run() {
        flushStats();
    }

    @Override
    public void increment(String metricName, int increment) {
        Integer currValue = metricMap.get(metricName);
        if (currValue == null) {
            currValue = 0;
        }
        currValue += increment;
        metricMap.put(metricName, currValue);
    }

    @Override
    public void setDefaultValue(String key, int val) {
        // TODO Auto-generated method stub

    }

    @Override
    public void increment(String metricName, int increment, String metricEventName, String... furtherDistinctions) {
        increment(buildMetricName(metricName,
                                  metricEventName,
                                  furtherDistinctions),
                  increment);

    }

    @Override
    public void set(String metricName, int value, String metricEventName, String... furtherDistinctions) {
        metricMap.put(buildMetricName(metricName,
                                      metricEventName,
                                      furtherDistinctions),
                      value);
    }

    private String buildMetricName(String metricName, String metricEventName, String[] furtherDistinctions) {
        StringBuffer sb = new StringBuffer(metricEventName);
        sb.append(":");
        sb.append(metricName);
        if (furtherDistinctions != null) {
            for (String furtherDistinction : furtherDistinctions) {
                sb.append(":");
                sb.append(furtherDistinction);
            }
        }
        return sb.toString().intern();

    }
}
