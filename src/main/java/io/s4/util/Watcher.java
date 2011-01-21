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

import static io.s4.util.MetricsName.S4_CORE_METRICS;
import static io.s4.util.MetricsName.s4_core_exit_ct;
import static io.s4.util.MetricsName.s4_core_free_mem;
import io.s4.logger.Monitor;
import io.s4.persist.Persister;
import io.s4.processor.AsynchronousEventProcessor;

import java.io.File;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;

public class Watcher implements Runnable {
    Runtime rt = Runtime.getRuntime();
    AsynchronousEventProcessor peContainer;
    Persister persister;
    Persister localPersister;
    String configFilename;
    long configFileTime = -1;
    Monitor monitor;

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    private long minimumMemory = 200 * 1024 * 1024;

    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

    public void setMinimumMemory(long minimumMemory) {
        this.minimumMemory = minimumMemory;
    }

    public void setPeContainer(AsynchronousEventProcessor peContainer) {
        this.peContainer = peContainer;
    }

    public void setPersister(Persister persister) {
        this.persister = persister;
    }

    public void setLocalPersister(Persister localPersister) {
        this.localPersister = localPersister;
    }

    public void setConfigFilename(String configFilename) {
        this.configFilename = configFilename;
    }

    public Watcher() {

    }

    public void init() {
        Thread t = new Thread(this);
        t.start();
    }

    public void run() {
        try {
            while (true) {
                String stringTime = dateFormatter.format(new Date());

                String template1 = "{0,number,#######0} waiting processing";
                Logger.getLogger("s4")
                      .info(MessageFormat.format(template1,
                                                 peContainer.getQueueSize()));

                String template2 = "Total: {0,number,#######0}, max {1,number,#######0}, free {2,number,#######0}";
                Logger.getLogger("s4")
                      .info(MessageFormat.format(template2,
                                                 rt.totalMemory(),
                                                 rt.maxMemory(),
                                                 rt.freeMemory()));
                memoryCheck();
                configCheck();
                try {
                    Thread.sleep(15000);
                } catch (Exception e) {
                }
            }
        } catch (Exception e) {
            Logger.getLogger("s4")
                  .warn("Some sort of exception in Watcher thread", e);
            try {
                Thread.sleep(30000);
            } catch (Exception ie) {
            }
        }
    }

    private ArrayList<byte[]> memoryHog = new ArrayList<byte[]>();

    private void memoryCheck() {
        long total = rt.totalMemory();
        long max = rt.maxMemory();
        long free = rt.freeMemory();
        long actualFree = (max - total) + free;

        try {
            if (monitor != null) {
                monitor.set(s4_core_free_mem.toString(),
                            (int) (actualFree / 1024 / 1024.0),
                            S4_CORE_METRICS.toString());
            }

            if (actualFree < minimumMemory) {
                Logger.getLogger("s4").error("Too little memory remaining: "
                        + actualFree + ". Exiting so process can be restarted");
                if (monitor != null) {
                    monitor.set(s4_core_exit_ct.toString(),
                                1,
                                S4_CORE_METRICS.toString());
                }
                System.exit(3);
            }
        } catch (Exception e) {
            Logger.getLogger("s4").error("metrics name doesn't exist: ", e);
        }

        // TODO: Comment this out!!
        // memoryHog.add(new byte[10*1024*1024]);
    }

    private void configCheck() {
        if (configFilename == null) {
            return;
        }

        File file = new File(configFilename);
        if (!file.exists()) {
            return;
        }
        long lastModified = file.lastModified();
        if (configFileTime == -1) {
            configFileTime = lastModified;
            return;
        }

        if (lastModified > configFileTime) {
            Logger.getLogger("s4").info("Config file has changed. Exiting!!");
            try {
                if (monitor != null) {
                    monitor.set(s4_core_exit_ct.toString(),
                                1,
                                S4_CORE_METRICS.toString());
                }
            } catch (Exception e) {
                Logger.getLogger("s4").error("metrics name doesn't exist: ", e);
            }
            System.exit(4);
        }
    }
}
