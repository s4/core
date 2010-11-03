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
package io.s4.collector;

import io.s4.listener.EventHandler;
import io.s4.logger.Monitor;
import io.s4.processor.PEContainer;
import io.s4.util.MetricsName;

import java.util.Map;

import org.apache.log4j.Logger;

import static io.s4.util.MetricsName.*;

public class EventListener implements EventHandler {
    private static Logger logger = Logger.getLogger(EventListener.class);
    private int eventCount = 0;
    private PEContainer peContainer;
    private io.s4.listener.EventListener rawListener;
    private Monitor monitor;

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    public void setPeContainer(PEContainer peContainer) {
        this.peContainer = peContainer;
    }

    public void setRawListener(io.s4.listener.EventListener rawListener) {
        this.rawListener = rawListener;
    }

    public int getEventCount() {
        return eventCount;
    }

    public EventListener() {

    }

    public void init() {
        rawListener.addHandler(this);
    }

    public void processEvent(EventWrapper eventWrapper) {
        try {
            synchronized (this) {
                eventCount++;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("STEP 3 (EventListener): peContainer.addEvent - "
                        + eventWrapper.getEvent().toString());
            }
            peContainer.queueWork(eventWrapper);

            if (monitor != null) {
                monitor.increment(generic_listener_msg_in_ct.toString(),
                                  1,
                                  S4_EVENT_METRICS.toString(),
                                  "et",
                                  eventWrapper.getStreamName());
                monitor.increment(generic_listener_msg_in_ct.toString(),
                                  1,
                                  S4_CORE_METRICS.toString());
            }
        } catch (Exception e) {
            logger.error("Exception in processEvent on thread "
                    + Thread.currentThread().getId(), e);
        }
    }
}
