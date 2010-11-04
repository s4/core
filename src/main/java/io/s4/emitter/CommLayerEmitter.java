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
package io.s4.emitter;

import io.s4.collector.EventWrapper;
import io.s4.listener.CommLayerListener;
import io.s4.logger.Monitor;
import io.s4.serialize.SerializerDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import io.s4.comm.core.SenderProcess;
import io.s4.comm.core.Serializer;

import static io.s4.util.MetricsName.*;

public class CommLayerEmitter implements EventEmitter, Runnable {
    private static Logger logger = Logger.getLogger(CommLayerEmitter.class);
    private CommLayerListener listener;
    private SenderProcess sender;
    private int nodeCount;
    private BlockingQueue<MessageHolder> messageQueue = new LinkedBlockingDeque<MessageHolder>();
    private String senderId;
    private String clusterManagerAddress;
    private String appName;
    private String listenerAppName;
    private Monitor monitor;
    private SerializerDeserializer serDeser;

    public void setSerDeser(SerializerDeserializer serDeser) {
        this.serDeser = serDeser;
    }

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public void setClusterManagerAddress(String clusterManagerAddress) {
        this.clusterManagerAddress = clusterManagerAddress;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setListenerAppName(String listenerAppName) {
        this.listenerAppName = listenerAppName;
    }

    public void setListener(CommLayerListener listener) {
        this.listener = listener;
    }

    public void init() {

        Thread t = new Thread(this, "CommLayerEmitter");
        t.start();
    }

    public void queueMessage(MessageHolder messageHolder) {
        messageQueue.add(messageHolder);
        try {
            if (monitor != null) {
                monitor.set(low_level_emitter_qsz.toString(),
                            messageQueue.size(),
                            S4_CORE_METRICS.toString());
            }
        } catch (Exception e) {
            logger.error("Exception in monitor metrics on thread "
                    + Thread.currentThread().getId(), e);
        }
    }

    @Override
    public void emit(int partitionId, EventWrapper eventWrapper) {
        try {
            byte[] rawMessage = serDeser.serialize(eventWrapper);
            MessageHolder mh = new MessageHolder(partitionId, rawMessage);
            queueMessage(mh);
        } catch (RuntimeException rte) {
            if (monitor != null) {
                monitor.increment(low_level_emitter_out_err_ct.toString(),
                                  1,
                                  S4_EVENT_METRICS.toString(),
                                  "et",
                                  eventWrapper.getStreamName());
            }
            Logger.getLogger("s4").error("Error serializing or emitting event "
                                                 + eventWrapper.getEvent(),
                                         rte);
            throw rte;
        }
    }

    @Override
    public int getNodeCount() {
        if (listener == null) {
            return 1;
        }
        return nodeCount;
    }

    @Override
    public void run() {
        if (listener == null) {
            if (listenerAppName == null) {
                listenerAppName = appName;
            }
            sender = new SenderProcess(clusterManagerAddress,
                                       appName,
                                       listenerAppName);
            Map<String, String> map = new HashMap<String, String>();
            map.put("SenderId", "" + senderId);
            sender.setSerializer(new PassThroughSerializer());
            sender.acquireTaskAndCreateSender(map);
        } else {
            Object listenerConfig = null;
            try {
                listenerConfig = listener.getListenerConfig();
                if (listenerConfig == null) {
                    logger.info("CommLayerEmitter going to wait for listener to acquire task");
                    synchronized (listener) {
                        listenerConfig = listener.getListenerConfig();
                        if (listenerConfig == null) {
                            listener.wait();
                            listenerConfig = listener.getListenerConfig();
                        }
                    }
                }
            } catch (Exception e) {
                logger.info("Exception in CommLayerEmitter.run()", e);
            }
            logger.info("Creating sender process with " + listenerConfig);
            sender = new SenderProcess(listener.getClusterManagerAddress(),
                                       listener.getAppName(),
                                       listener.getAppName());
            sender.setSerializer(new PassThroughSerializer());
            sender.createSenderFromConfig(listenerConfig);
            nodeCount = sender.getNumOfPartitions();
        }
        boolean isSent = false;
        while (!Thread.interrupted()) {
            isSent = false;
            try {
                MessageHolder mh = messageQueue.take();
                byte[] rawMessage = mh.getRawMessage();
                if (listener == null) {
                    isSent = sender.send(rawMessage);
                } else {
                    isSent = sender.sendToPartition(mh.getPartitionId(),
                                                    rawMessage);
                }

                if (isSent) {
                    if (monitor != null) {
                        monitor.increment(low_level_emitter_msg_out_ct.toString(),
                                          1,
                                          S4_CORE_METRICS.toString());
                    }
                } else {
                    if (monitor != null) {
                        monitor.increment(low_level_emitter_out_err_ct.toString(),
                                          1,
                                          S4_CORE_METRICS.toString());
                    }
                    logger.warn("commlayer emit failed ...");
                }
            } catch (InterruptedException ie) {
                if (monitor != null) {
                    monitor.increment(low_level_emitter_out_err_ct.toString(),
                                      1,
                                      S4_CORE_METRICS.toString());
                }
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                Logger.getLogger("s4").error("Error emitting message", e);
            }
        }
    }

    public class PassThroughSerializer implements Serializer {
        public byte[] serialize(Object input) {
            if (input instanceof byte[]) {
                return (byte[]) input;
            } else {
                return null;
            }
        }
    }

    class MessageHolder {
        private int partitionId;
        private byte[] rawMessage;

        MessageHolder(int partitionId, byte[] rawMessage) {
            this.partitionId = partitionId;
            this.rawMessage = rawMessage;
        }

        int getPartitionId() {
            return partitionId;
        }

        byte[] getRawMessage() {
            return rawMessage;
        }
    }
}
