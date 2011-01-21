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
package io.s4.adapter;

import io.s4.collector.EventWrapper;
import io.s4.dispatcher.EventDispatcher;
import io.s4.listener.EventHandler;
import io.s4.listener.EventListener;
import io.s4.util.S4Util;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class Adapter implements EventHandler {
    private static String coreHome = "../s4_core";

    private EventDispatcher dispatcher;
    private EventListener[] eventListeners;
    private String configFilename;

    public void setDispatcher(EventDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void setEventListeners(EventListener[] eventListeners) {
        this.eventListeners = eventListeners;
        for (EventListener eventListener : eventListeners) {
            eventListener.addHandler(this);
        }
    }

    public void setConfigFilename(String configFilename) {
        this.configFilename = configFilename;
    }

    private volatile int eventCount = 0;
    private volatile int rawEventCount = 0;

    public Adapter() {

    }

    int counts[];

    private boolean init = false;

    public void init() {
        synchronized (this) {
            init = true;
        }
    }

    public void processEvent(EventWrapper eventWrapper) {
        try {
            synchronized (this) {
                if (!init) {
                    return;
                }
                rawEventCount++;
                eventCount++;
            }
            dispatcher.dispatchEvent(eventWrapper.getStreamName(),
                                     eventWrapper.getEvent());
        } catch (Exception e) {
            Logger.getLogger("dispatcher").info("Exception adapting event", e);
        }
    }

    public static void main(String args[]) {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("corehome")
                                       .hasArg()
                                       .withDescription("core home")
                                       .create("c"));

        options.addOption(OptionBuilder.withArgName("instanceid")
                                       .hasArg()
                                       .withDescription("instance id")
                                       .create("i"));

        options.addOption(OptionBuilder.withArgName("configtype")
                                       .hasArg()
                                       .withDescription("configuration type")
                                       .create("t"));

        options.addOption(OptionBuilder.withArgName("userconfig")
                                       .hasArg()
                                       .withDescription("user-defined legacy data adapter configuration file")
                                       .create("d"));

        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = null;

        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException pe) {
            System.err.println(pe.getLocalizedMessage());
            System.exit(1);
        }

        int instanceId = -1;
        if (commandLine.hasOption("i")) {
            String instanceIdStr = commandLine.getOptionValue("i");
            try {
                instanceId = Integer.parseInt(instanceIdStr);
            } catch (NumberFormatException nfe) {
                System.err.println("Bad instance id: %s" + instanceIdStr);
                System.exit(1);
            }
        }

        if (commandLine.hasOption("c")) {
            coreHome = commandLine.getOptionValue("c");
        }

        String configType = "typical";
        if (commandLine.hasOption("t")) {
            configType = commandLine.getOptionValue("t");
        }

        String userConfigFilename = null;
        if (commandLine.hasOption("d")) {
            userConfigFilename = commandLine.getOptionValue("d");
        }

        File userConfigFile = new File(userConfigFilename);
        if (!userConfigFile.isFile()) {
            System.err.println("Bad user configuration file: "
                    + userConfigFilename);
            System.exit(1);
        }

        File coreHomeFile = new File(coreHome);
        if (!coreHomeFile.isDirectory()) {
            System.err.println("Bad core home: " + coreHome);
            System.exit(1);
        }

        if (instanceId > -1) {
            System.setProperty("instanceId", "" + instanceId);
        } else {
            System.setProperty("instanceId", "" + S4Util.getPID());
        }

        String configBase = coreHome + File.separatorChar + "conf"
                + File.separatorChar + configType;
        String configPath = configBase + File.separatorChar
                + "adapter_conf.xml";
        File configFile = new File(configPath);
        if (!configFile.exists()) {
            System.err.printf("adapter config file %s does not exist\n",
                              configPath);
            System.exit(1);
        }

        // load adapter config xml
        ApplicationContext coreContext;
        coreContext = new FileSystemXmlApplicationContext("file:" + configPath);
        ApplicationContext context = coreContext;

        Adapter adapter = (Adapter) context.getBean("adapter");

        ApplicationContext appContext = new FileSystemXmlApplicationContext(new String[] { "file:"
                                                                                    + userConfigFilename },
                                                                            context);
        Map listenerBeanMap = appContext.getBeansOfType(EventListener.class);
        if (listenerBeanMap.size() == 0) {
            System.err.println("No user-defined listener beans");
            System.exit(1);
        }
        EventListener[] eventListeners = new EventListener[listenerBeanMap.size()];

        int index = 0;
        for (Iterator it = listenerBeanMap.keySet().iterator(); it.hasNext(); index++) {
            String beanName = (String) it.next();
            System.out.println("Adding listener " + beanName);
            eventListeners[index] = (EventListener) listenerBeanMap.get(beanName);
        }

        adapter.setEventListeners(eventListeners);
    }
}
