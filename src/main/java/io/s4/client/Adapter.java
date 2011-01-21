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
package io.s4.client;

import io.s4.collector.EventListener;
import io.s4.collector.EventWrapper;
import io.s4.dispatcher.EventDispatcher;
import io.s4.listener.EventHandler;
import io.s4.processor.AsynchronousEventProcessor;
import io.s4.util.S4Util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * Adapter to connect an S4 cluster to an external client to read from and write
 * into the S4 cluster.
 */
public class Adapter {
    private static String coreHome = "../s4_core";

    // Accept events from clients and send into S4 cluster.
    private EventDispatcher dispatcher;
    private Writer eventWriter = new Writer();

    // Listen to events from S4 cluster and send to clients.
    private io.s4.collector.EventListener clusterEventListener;
    private Reader eventReader = new Reader();

    /**
     * Set the dispatcher to use for sending events into S4.
     * 
     * @param dispatcher
     */
    public void setDispatcher(EventDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    /**
     * clusterEventListener receives events from S4 cluster. Processing of
     * events is delegated to the eventReader, which typically forwards them to
     * appropriate clients.
     * 
     * @param clusterEventListener
     */
    public void setClusterEventListener(EventListener clusterEventListener) {
        this.clusterEventListener = clusterEventListener;
        this.clusterEventListener.setEventProcessor(eventReader);
    }

    public Adapter() {
    }

    int counts[];

    private boolean init = false;

    public void init() {
        synchronized (this) {
            init = true;
        }
    }

    /**
     * Register a list of InputStubs that will send events into the S4 cluster.
     * These events will be processed by the eventWriter: i.e. the eventWriter
     * dispatches them into the S4 cluster.
     * 
     * @param stubs
     */
    public void setInputStubs(List<InputStub> stubs) {
        for (InputStub stub : stubs) {
            // register the writer as the handler for the stub's events
            stub.addHandler(eventWriter);
        }
    }

    // collections of eventReceivers (OutputStubs) to which events will be sent
    // for forwarding to clients.
    HashMap<String, List<OutputStub>> eventReceivers = new HashMap<String, List<OutputStub>>();
    List<OutputStub> eventReceiversAny = new ArrayList<OutputStub>();
    List<OutputStub> eventReceiversAll = new ArrayList<OutputStub>();

    /**
     * Register a list of OutputStubs which process events received from the S4
     * cluster, typically forwarding them to clients.
     * 
     * The {@link OutputStub#getAcceptedStreams()} is called to determine which
     * streams the stub is interested in receiving. Accordingly, three
     * collections of stubs are created.
     * 
     * <ol>
     * <li>A mapping from stream name to OutputStubs (One-to-many). Used for
     * routing.</li>
     * <li>A list of OutputStubs that accept all streams (
     * {@code stub.getAcceptedStreams() == null}. Used for routing.</li>
     * <li>A list of all OutputStubs. This is used to iterate over all the stubs
     * exactly once.</li>
     * </ol>
     * 
     * @param stubs
     *            the list of output stubs.
     */
    public void setOutputStubs(List<OutputStub> stubs) {
        eventReceiversAll.addAll(stubs);

        for (OutputStub stub : stubs) {
            // update mapping of stream names to stubs that accept events on
            // that stream.
            List<String> streams = stub.getAcceptedStreams();
            if (streams != null) {
                for (String stream : streams) {
                    List<OutputStub> stubList = eventReceivers.get(stream);
                    if (stubList == null) {
                        stubList = new ArrayList<OutputStub>();
                        eventReceivers.put(stream, stubList);
                    }

                    stubList.add(stub);
                }
            } else {
                eventReceiversAny.add(stub);
            }
        }
    }

    /**
     * Write events from input stubs into S4 cluster.
     */
    private class Writer implements EventHandler {
        // events to be dispatched into cluster
        public void processEvent(EventWrapper eventWrapper) {
            try {
                synchronized (this) {
                    if (!init) {
                        return;
                    }
                    rawEventCount++;
                    eventCount++;
                }

                // null keys => round-robin
                // empty key list => default partitioning of underlying
                // partitioner.
                List<List<String>> keys = eventWrapper.getCompoundKeyNames();

                String stream = eventWrapper.getStreamName();

                Object event = eventWrapper.getEvent();

                dispatcher.dispatchEvent(stream, keys, event);

            } catch (Exception e) {
                Logger.getLogger("adapter").info("Exception adapting event",
                                                    e);
            }
        }

        private volatile int eventCount = 0;
        private volatile int rawEventCount = 0;
    }

    /**
     * Read events from S4 cluster.
     */
    private class Reader implements AsynchronousEventProcessor {
        /**
         * Queue work for processing by OutputStubs. This method simply queues
         * the events in the appropriate stubs. An event from stream K is queued
         * in OutputStub S if either S accepts all streams, or K is contained in
         * the list {@code S.getAcceptedStreams()}.
         */
        @Override
        public void queueWork(EventWrapper eventWrapper) {
            List<OutputStub> stubs = eventReceivers.get(eventWrapper.getStreamName());

            // stubs that accept any stream
            for (OutputStub stub : eventReceiversAny)
                stub.queueWork(eventWrapper);

            // stubs that receive this stream in particular
            if (stubs != null)
                for (OutputStub stub : stubs)
                    stub.queueWork(eventWrapper);
        }

        @Override
        public int getQueueSize() {
            int sz = 0;

            for (OutputStub stub : eventReceiversAll)
                sz += stub.getQueueSize();

            return sz;
        }

    }

    private static class TestDispatcher implements EventDispatcher {
        @Override
        public void dispatchEvent(String s, Object e) {
            System.out.println("Dispatching event: " + s + ":" + e);
        }

        @Override
        public void dispatchEvent(String s, List<List<String>> k, Object e) {
            System.out.println("Dispatching event: " + s + ":" + k + ":" + e);
        }
    }

    private static class TestReturnType {
        private int ra;
        private int rb;

        @SuppressWarnings("unused")
        TestReturnType() {
        }

        TestReturnType(int a, int b) {
            this.ra = a;
            this.rb = b;
        }

        public String toString() {
            return "ra=" + ra + " rb=" + rb;
        }
    }

    @SuppressWarnings("static-access")
    public static void main(String args[]) throws IOException,
            InterruptedException {

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
                + "client_adapter_conf.xml";
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

        Adapter adapter = (Adapter) context.getBean("client_adapter");

        ApplicationContext appContext = new FileSystemXmlApplicationContext(new String[] { "file:"
                                                                                    + userConfigFilename },
                                                                            context);

        Map<?, ?> inputStubBeanMap = appContext.getBeansOfType(InputStub.class);
        Map<?, ?> outputStubBeanMap = appContext.getBeansOfType(OutputStub.class);

        if (inputStubBeanMap.size() == 0 && outputStubBeanMap.size() == 0) {
            System.err.println("No user-defined input/output stub beans");
            System.exit(1);
        }

        ArrayList<InputStub> inputStubs = new ArrayList<InputStub>(inputStubBeanMap.size());
        ArrayList<OutputStub> outputStubs = new ArrayList<OutputStub>(outputStubBeanMap.size());

        // add all input stubs
        for (Map.Entry<?, ?> e : inputStubBeanMap.entrySet()) {
            String beanName = (String) e.getKey();
            System.out.println("Adding InputStub " + beanName);
            inputStubs.add((InputStub) e.getValue());
        }

        // add all output stubs
        for (Map.Entry<?, ?> e : outputStubBeanMap.entrySet()) {
            String beanName = (String) e.getKey();
            System.out.println("Adding OutputStub " + beanName);
            outputStubs.add((OutputStub) e.getValue());
        }

        adapter.setInputStubs(inputStubs);
        adapter.setOutputStubs(outputStubs);

    }

    public static void clientTest() throws IOException, InterruptedException {
        BasicConfigurator.configure();

        TestDispatcher disp = new TestDispatcher();

        Adapter adapter = new Adapter();
        adapter.setDispatcher(disp);

        GenericJsonClientStub stub = new GenericJsonClientStub();
        stub.setConnectionPort(2334);

        InputStub[] in = { stub };
        OutputStub[] out = { stub };
        adapter.setInputStubs(Arrays.asList(in));
        adapter.setOutputStubs(Arrays.asList(out));

        adapter.init();
        stub.init();

        while (true) {
            Thread.sleep(10000);
            TestReturnType r = new TestReturnType(100, 200);
            adapter.eventReader.queueWork(new EventWrapper("TESTSTREAM",
                                                           r,
                                                           null));
        }
    }
}
