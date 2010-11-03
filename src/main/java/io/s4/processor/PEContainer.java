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
package io.s4.processor;

import io.s4.collector.Event;
import io.s4.collector.EventWrapper;
import io.s4.dispatcher.partitioner.CompoundKeyInfo;
import io.s4.logger.Monitor;
import io.s4.util.MetricsName;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;

import static io.s4.util.MetricsName.*;

public class PEContainer implements Runnable {
    private static Logger logger = Logger.getLogger(PEContainer.class);
    BlockingQueue<EventWrapper> workQueue;
    private List<PrototypeWrapper> prototypeWrappers = new ArrayList<PrototypeWrapper>();
    private Monitor monitor;
    private int maxQueueSize = 1000;
    private boolean trackByKey;
    private Map<String, Integer> countByEventType = Collections.synchronizedMap(new HashMap<String, Integer>());

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    public void setTrackByKey(boolean trackByKey) {
        this.trackByKey = trackByKey;
    }

    public void addProcessor(ProcessingElement processor) {
        System.out.println("adding pe: " + processor);
        PrototypeWrapper pw = new PrototypeWrapper(processor);
        prototypeWrappers.add(pw);
        adviceLists.add(pw.advise());
    }

    public void setProcessors(ProcessingElement[] processors) {
        // prototypeWrappers = new ArrayList<PrototypeWrapper>();

        for (int i = 0; i < processors.length; i++) {
            prototypeWrappers.add(new PrototypeWrapper(processors[i]));
        }
    }

    public PEContainer() {

    }

    List<List<EventAdvice>> adviceLists = new ArrayList<List<EventAdvice>>();

    public void init() {
        workQueue = new LinkedBlockingQueue<EventWrapper>(maxQueueSize);
        for (PrototypeWrapper pw : prototypeWrappers) {
            adviceLists.add(pw.advise());
        }
        Thread t = new Thread(this, "PEContainer");
        t.start();
        t = new Thread(new Watcher());
        t.start();
    }

    public void queueWork(EventWrapper eventWrapper) {
        boolean isAddSucceed = false;

        try {
            isAddSucceed = workQueue.offer(eventWrapper);
            if (monitor != null) {
                if (isAddSucceed) {
                    monitor.increment(pecontainer_ev_nq_ct.toString(),
                                      1,
                                      S4_CORE_METRICS.toString());
                } else {
                    monitor.increment(pecontainer_msg_drop_ct.toString(),
                                      1,
                                      S4_CORE_METRICS.toString());
                }
                monitor.set(pecontainer_qsz.toString(),
                            getQueueSize(),
                            S4_CORE_METRICS.toString());
            }
        } catch (Exception e) {
            logger.error("metrics name doesn't exist", e);
        }
    }

    // This will always be called by a different thread than the one executing
    // run()
    public int getQueueSize() {
        return workQueue.size();
    }

    public void run() {
        long startTime, endTime;
        while (true) {
            EventWrapper eventWrapper = null;
            try {
                eventWrapper = workQueue.take();

                if (trackByKey) {
                    boolean foundOne = false;
                    for (CompoundKeyInfo compoundKeyInfo : eventWrapper.getCompoundKeys()) {
                        foundOne = true;
                        updateCount(eventWrapper.getStreamName() + " "
                                + compoundKeyInfo.getCompoundKey());
                    }

                    if (!foundOne) {
                        updateCount(eventWrapper.getStreamName() + " *");
                    }
                }

                startTime = System.currentTimeMillis();
                if (logger.isDebugEnabled()) {
                    logger.debug("STEP 5 (PEContainer): workQueue.take - "
                            + eventWrapper.toString());
                }
                // Logger.getLogger("s4").debug(
                // "Incoming: " + event.getEventName());
                if (monitor != null) {
                    monitor.increment(pecontainer_ev_dq_ct.toString(),
                                      1,
                                      S4_CORE_METRICS.toString());
                }
                // printPlainPartitionInfoList(event.getCompoundKeyList());
                // execute the PEs interested in this event
                for (int i = 0; i < prototypeWrappers.size(); i++) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("STEP 6 (PEContainer): prototypeWrappers("
                                + i + ") - "
                                + prototypeWrappers.get(i).toString() + " - "
                                + eventWrapper.getStreamName());
                    }
                    List<EventAdvice> adviceList = adviceLists.get(i);
                    for (EventAdvice eventAdvice : adviceList) {
                        if (eventAdvice.getEventName().equals("*")
                                || eventAdvice.getEventName()
                                              .equals(eventWrapper.getStreamName())) {
                            // event name matches
                        } else {
                            continue;
                        }

                        if (eventAdvice.getKey().equals("*")) {
                            invokePE(prototypeWrappers.get(i).getPE("*"),
                                     eventWrapper,
                                     null);
                            continue;
                        }

                        for (CompoundKeyInfo compoundKeyInfo : eventWrapper.getCompoundKeys()) {
                            if (eventAdvice.getKey()
                                           .equals(compoundKeyInfo.getCompoundKey())) {
                                invokePE(prototypeWrappers.get(i)
                                                          .getPE(compoundKeyInfo.getCompoundValue()),
                                         eventWrapper,
                                         compoundKeyInfo);
                            }
                        }
                    }
                }
                endTime = System.currentTimeMillis();
                if (monitor != null) {
                    // TODO: need to be changed for more accurate calc
                    monitor.increment(pecontainer_exec_elapse_time.toString(),
                                      (int) (endTime - startTime),
                                      S4_CORE_METRICS.toString());
                }
            } catch (InterruptedException ie) {
                Logger.getLogger("s4").warn("PEContainer is interrupted", ie);
                return;
            } catch (Exception e) {
                Logger.getLogger("s4")
                      .error("Exception choosing processing element to run", e);
            }
        }
    }

    private void invokePE(ProcessingElement pe, EventWrapper eventWrapper, CompoundKeyInfo compoundKeyInfo) {
        try {
            long startTime = System.currentTimeMillis();
            pe.execute(eventWrapper.getStreamName(),
                       compoundKeyInfo,
                       eventWrapper.getEvent());
            long endTime = System.currentTimeMillis();
            if (monitor != null) {
                monitor.increment(pecontainer_ev_process_ct.toString(),
                                  1,
                                  S4_CORE_METRICS.toString());
                monitor.increment(pecontainer_ev_process_ct.toString(),
                                  1,
                                  S4_APP_METRICS.toString(),
                                  "at",
                                  pe.getId());
                monitor.increment(pecontainer_exec_elapse_time.toString(),
                                  (int) (endTime - startTime),
                                  S4_APP_METRICS.toString(),
                                  "at",
                                  pe.getId());
            }
        } catch (Exception e) {
            if (monitor != null) {
                monitor.increment(pecontainer_ev_err_ct.toString(),
                                  1,
                                  S4_CORE_METRICS.toString());
                monitor.increment(pecontainer_ev_err_ct.toString(),
                                  1,
                                  S4_APP_METRICS.toString(),
                                  "at",
                                  pe.getId());
            }
            Logger.getLogger("s4")
                  .error("Exception running processing element", e);
        }

    }

    private void updateCount(String key) {
        Integer countObj = countByEventType.get(key);
        if (countObj == null) {
            countObj = 0;
        }
        countObj++;
        countByEventType.put(key, countObj);

    }

    class Watcher implements Runnable {
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    int peCount = 0;
                    for (PrototypeWrapper pw : prototypeWrappers) {
                        peCount += pw.getPECount();
                        if (monitor != null) {
                            monitor.set(pecontainer_pe_ct.toString(),
                                        pw.getPECount(),
                                        S4_APP_METRICS.toString(),
                                        "at",
                                        pw.getId());
                        }
                    }

                    Logger.getLogger("s4").info("PE count " + peCount);
                    if (monitor != null) {
                        monitor.set(pecontainer_pe_ct.toString(),
                                    peCount,
                                    S4_CORE_METRICS.toString());
                        monitor.set(pecontainer_qsz_w.toString(),
                                    getQueueSize(),
                                    S4_CORE_METRICS.toString());
                    }

                    if (trackByKey) {
                        for (String key : countByEventType.keySet()) {
                            Integer countObj = countByEventType.get(key);
                            if (countObj != null) {
                                Logger.getLogger("s4").info("Count by " + key
                                        + ": " + countObj);
                            }
                        }
                    }
                } catch (Exception e) {
                    Logger.getLogger("s4")
                          .error("Exception running PEContainer watcher", e);
                } finally {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }
}
