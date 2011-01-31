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
package io.s4.listener;

import static io.s4.util.MetricsName.S4_CORE_METRICS;
import static io.s4.util.MetricsName.low_level_listener_badmsg_ct;
import static io.s4.util.MetricsName.low_level_listener_msg_drop_ct;
import static io.s4.util.MetricsName.low_level_listener_msg_in_ct;
import static io.s4.util.MetricsName.low_level_listener_qsz;
import static io.s4.util.MetricsName.s4_core_exit_ct;
import io.s4.collector.EventWrapper;
import io.s4.comm.core.CommEventCallback;
import io.s4.comm.core.CommLayerState;
import io.s4.comm.core.Deserializer;
import io.s4.comm.core.ListenerProcess;
import io.s4.logger.Monitor;
import io.s4.serialize.SerializerDeserializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

public class CommLayerListener implements EventListener, Runnable {
    private static Logger logger = Logger.getLogger(CommLayerListener.class);
    private int dequeuerCount = 12;
    private Set<EventHandler> handlers = new HashSet<EventHandler>();
    ListenerProcess process;
    private BlockingQueue<Object> messageQueue;
    private int maxQueueSize = 1000;
    private String clusterManagerAddress;
    private String appName;
    private Object listenerConfig;
    private Monitor monitor;
    private int partitionId = -1;
    private int zkConnected = 1;
    private SerializerDeserializer serDeser;

    public void setSerDeser(SerializerDeserializer serDeser) {
        this.serDeser = serDeser;
    }

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
        monitor.setDefaultValue("tid", partitionId);
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    @Override
    public int getId() {
        return partitionId;
    }

    @Override
    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getClusterManagerAddress() {
        return clusterManagerAddress;
    }

    public void setClusterManagerAddress(String clusterManagerAddress) {
        this.clusterManagerAddress = clusterManagerAddress;
    }

    @Override
    public void addHandler(EventHandler handler) {
        handlers.add(handler);
    }

    @Override
    public boolean removeHandler(EventHandler handler) {
        return handlers.remove(handler);
    }

    public Object getListenerConfig() {
        return this.listenerConfig;
    }

    public void init() {
        System.err.println("appName=" + appName);
        process = new ListenerProcess(clusterManagerAddress, appName);
        process.setDeserializer(new PassThroughDeserializer());
        CommEventCallback callbackHandler = new CommEventCallback() {
            @Override
            public void handleCallback(Map<String, Object> event) {
                if (event != null) {
                    CommLayerState state = (CommLayerState) event.get("state");
                    if (state != null) {
                        if (state == CommLayerState.INITIALIZED) {
                            logger.info("Communication layer initialized: source:"
                                    + event.get("source"));
                        } else if (state == CommLayerState.BROKEN) {
                            logger.error("Communication layer broken: source:"
                                    + event.get("source"));
                            logger.error("System exiting so that process can restart.");
                            if (monitor != null) {
                                monitor.set(s4_core_exit_ct.toString(),
                                            1,
                                            S4_CORE_METRICS.toString());
                            }
                            // should flush stats before exiting
                            monitor.flushStats();
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                            }
                            System.exit(3);
                        }
                    }
                }
            }
        };
        process.setCallbackHandler(callbackHandler);

        messageQueue = new LinkedBlockingQueue<Object>(maxQueueSize);

        // listenerConfig = process.acquireTaskAndCreateListener(map);
        Thread t = new Thread(this);
        t.setPriority(Thread.MAX_PRIORITY);
        t.start();

        if (System.getProperty("DequeuerCount") != null) {
            dequeuerCount = Integer.parseInt(System.getProperty("DequeuerCount"));
        }

        System.out.println("dequeuer number: " + dequeuerCount);

        for (int i = 0; i < dequeuerCount; i++) {
            t = new Thread(new Dequeuer(this, i));
            // t.setPriority(Thread.MIN_PRIORITY);
            t.start();
        }
    }

    // This is the actual raw listener, which simply listens for messages on the
    // socket
    public void run() {
        boolean isAddMessageSucceeded = false;
        // acquire a task to do
        synchronized (this) {
            Map<String, String> map = new HashMap<String, String>();
            try {
                map.put("ListenerId", InetAddress.getLocalHost().getHostName()
                        + "_" + System.getProperty("pid") + "_"
                        + Thread.currentThread().getId());
                map.put("address", InetAddress.getLocalHost().getHostAddress());
            } catch (UnknownHostException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            logger.info("Waiting to acquire task");
            listenerConfig = process.acquireTaskAndCreateListener(map);
            logger.info("acquired task with config:" + listenerConfig);
            Map<String, String> configMap = (Map<String, String>) listenerConfig;
            String partition = configMap.get("partition");
            if (partition != null) {
                partitionId = Integer.parseInt(partition);
                monitor.setDefaultValue("tid", partitionId);
                logger.info("tid is set to " + partitionId);
            }
            this.notify();
        }
        while (!Thread.interrupted()) {
            byte[] message = (byte[]) process.listen();

            try {
                isAddMessageSucceeded = messageQueue.offer(message);
                if (monitor != null) {
                    monitor.set(low_level_listener_qsz.toString(),
                                messageQueue.size(),
                                S4_CORE_METRICS.toString());
                    if (isAddMessageSucceeded) {
                        monitor.increment(low_level_listener_msg_in_ct.toString(),
                                          1,
                                          S4_CORE_METRICS.toString());
                    } else {
                        monitor.increment(low_level_listener_msg_drop_ct.toString(),
                                          1,
                                          S4_CORE_METRICS.toString());
                    }
                }
            } catch (Exception e) {
                Logger.getLogger("s4")
                      .error("Exception in monitor metrics on thread "
                                     + Thread.currentThread().getId(),
                             e);
            }
        }
    }

    public Object takeMessage() throws InterruptedException {
        return messageQueue.take();
    }

    class Dequeuer implements Runnable {
        private int id;
        private CommLayerListener rawListener;

        public Dequeuer(CommLayerListener rawListener, int id) {
            this.id = id;
            this.rawListener = rawListener;
        }

        public void run() {
            while (!Thread.interrupted()) {
                try {
                    byte[] rawMessage = (byte[]) rawListener.takeMessage();
                    processMessage(rawMessage);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public void processMessage(byte[] rawMessage) {
            // convert the byte array into an event object
            EventWrapper eventWrapper = null;
            try {
                eventWrapper = (EventWrapper) serDeser.deserialize(rawMessage);

            } catch (RuntimeException rte) {
                Logger.getLogger("s4")
                      .error("Error converting message to an event: ", rte);
                if (monitor != null) {
                    monitor.increment(low_level_listener_badmsg_ct.toString(),
                                      1,
                                      S4_CORE_METRICS.toString());
                }
                return;
            }

            if (eventWrapper != null) {
                for (EventHandler handler : handlers) {
                    try {
                        handler.processEvent(eventWrapper);
                    } catch (Exception e) {
                        Logger.getLogger("s4")
                              .error("Error calling processEvent on handler", e);
                    }
                }
            }
        }

    }

    public class PassThroughDeserializer implements Deserializer {
        public Object deserialize(byte[] input) {
            return input;
        }
    }
}
