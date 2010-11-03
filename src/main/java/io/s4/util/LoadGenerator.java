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

import io.s4.collector.EventWrapper;
import io.s4.dispatcher.partitioner.CompoundKeyInfo;
import io.s4.emitter.CommLayerEmitter;
import io.s4.emitter.EventEmitter;
import io.s4.schema.Schema;
import io.s4.schema.Schema.Property;
import io.s4.serialize.KryoSerDeser;
import io.s4.serialize.SerializerDeserializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class LoadGenerator {

    public static void main(String args[]) {
        Options options = new Options();
        boolean warmUp = false;

        options.addOption(OptionBuilder.withArgName("rate")
                                       .hasArg()
                                       .withDescription("Rate (events per second)")
                                       .create("r"));

        options.addOption(OptionBuilder.withArgName("display_rate")
                                       .hasArg()
                                       .withDescription("Display Rate at specified second boundary")
                                       .create("d"));

        options.addOption(OptionBuilder.withArgName("start_boundary")
                                       .hasArg()
                                       .withDescription("Start boundary in seconds")
                                       .create("b"));

        options.addOption(OptionBuilder.withArgName("run_for")
                                       .hasArg()
                                       .withDescription("Run for a specified number of seconds")
                                       .create("x"));

        options.addOption(OptionBuilder.withArgName("cluster_manager")
                                       .hasArg()
                                       .withDescription("Cluster manager")
                                       .create("z"));

        options.addOption(OptionBuilder.withArgName("sender_application_name")
                                       .hasArg()
                                       .withDescription("Sender application name")
                                       .create("a"));

        options.addOption(OptionBuilder.withArgName("listener_application_name")
                                       .hasArg()
                                       .withDescription("Listener application name")
                                       .create("g"));

        options.addOption(OptionBuilder.withArgName("sleep_overhead")
                                       .hasArg()
                                       .withDescription("Sleep overhead")
                                       .create("o"));

        options.addOption(new Option("w", "Warm-up"));

        CommandLineParser parser = new GnuParser();

        CommandLine line = null;
        try {
            // parse the command line arguments
            line = parser.parse(options, args);
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            System.exit(1);
        }

        int expectedRate = 250;
        if (line.hasOption("r")) {
            try {
                expectedRate = Integer.parseInt(line.getOptionValue("r"));
            } catch (Exception e) {
                System.err.println("Bad expected rate specified "
                        + line.getOptionValue("r"));
                System.exit(1);
            }
        }

        int displayRateIntervalSeconds = 20;
        if (line.hasOption("d")) {
            try {
                displayRateIntervalSeconds = Integer.parseInt(line.getOptionValue("d"));
            } catch (Exception e) {
                System.err.println("Bad display rate value specified "
                        + line.getOptionValue("d"));
                System.exit(1);
            }
        }

        int startBoundary = 2;
        if (line.hasOption("b")) {
            try {
                startBoundary = Integer.parseInt(line.getOptionValue("b"));
            } catch (Exception e) {
                System.err.println("Bad start boundary value specified "
                        + line.getOptionValue("b"));
                System.exit(1);
            }
        }

        int updateFrequency = 0;
        if (line.hasOption("f")) {
            try {
                updateFrequency = Integer.parseInt(line.getOptionValue("f"));
            } catch (Exception e) {
                System.err.println("Bad query udpdate frequency specified "
                        + line.getOptionValue("f"));
                System.exit(1);
            }
            System.out.printf("Update frequency is %d\n", updateFrequency);
        }

        int runForTime = 0;
        if (line.hasOption("x")) {
            try {
                runForTime = Integer.parseInt(line.getOptionValue("x"));
            } catch (Exception e) {
                System.err.println("Bad run for time specified "
                        + line.getOptionValue("x"));
                System.exit(1);
            }
            System.out.printf("Run for time is %d\n", runForTime);
        }

        String clusterManagerAddress = null;
        if (line.hasOption("z")) {
            clusterManagerAddress = line.getOptionValue("z");
        }

        String senderApplicationName = null;
        if (line.hasOption("a")) {
            senderApplicationName = line.getOptionValue("a");
        }

        String listenerApplicationName = null;
        if (line.hasOption("a")) {
            listenerApplicationName = line.getOptionValue("g");
        }

        if (listenerApplicationName == null) {
            listenerApplicationName = senderApplicationName;
        }

        long sleepOverheadMicros = -1;
        if (line.hasOption("o")) {
            try {
                sleepOverheadMicros = Long.parseLong(line.getOptionValue("o"));
            } catch (NumberFormatException e) {
                System.err.println("Bad sleep overhead specified "
                        + line.getOptionValue("o"));
                System.exit(1);
            }
            System.out.printf("Specified sleep overhead is %d\n",
                              sleepOverheadMicros);
        }

        if (line.hasOption("w")) {
            warmUp = true;
        }

        List loArgs = line.getArgList();
        if (loArgs.size() < 1) {
            System.err.println("No input file specified");
            System.exit(1);
        }

        String inputFilename = (String) loArgs.get(0);

        EventEmitter emitter = null;

        SerializerDeserializer serDeser = new KryoSerDeser();

        CommLayerEmitter clEmitter = new CommLayerEmitter();
        clEmitter.setAppName(senderApplicationName);
        clEmitter.setListenerAppName(listenerApplicationName);
        clEmitter.setClusterManagerAddress(clusterManagerAddress);
        clEmitter.setSenderId(String.valueOf(System.currentTimeMillis() / 1000));
        clEmitter.setSerDeser(serDeser);
        clEmitter.init();
        emitter = clEmitter;

        long endTime = 0;
        if (runForTime > 0) {
            endTime = System.currentTimeMillis() + (runForTime * 1000);
        }

        LoadGenerator loadGenerator = new LoadGenerator();
        loadGenerator.setInputFilename(inputFilename);
        loadGenerator.setEventEmitter(clEmitter);
        loadGenerator.setDisplayRateInterval(displayRateIntervalSeconds);
        loadGenerator.setExpectedRate(expectedRate);
        loadGenerator.run();

        System.exit(0);
    }

    private EventEmitter eventEmitter;
    private String inputFilename;
    private int emitCount;
    private int displayRateInterval = 0;

    private int expectedRate = 200;
    private int adjustedExpectedRate = 1;
    private long sleepOverheadMicros = -1;
    private static int PROCESS_TIME_LIST_MAX_SIZE = 15;
    private long[] processTimes = new long[PROCESS_TIME_LIST_MAX_SIZE];
    private int processTimePointer = 0;
    private Map<Integer, EventTypeInfo> eventTypeInfoMap = new HashMap<Integer, EventTypeInfo>();

    public int getEmitCount() {
        return emitCount;
    }

    public void setEventEmitter(EventEmitter eventEmitter) {
        this.eventEmitter = eventEmitter;
    }

    public void setInputFilename(String inputFilename) {
        this.inputFilename = inputFilename;
    }

    public void setDisplayRateInterval(int displayRateInterval) {
        this.displayRateInterval = displayRateInterval;
    }

    public void setSleepOverheadMicros(long sleepOverheadMicros) {
        this.sleepOverheadMicros = sleepOverheadMicros;
    }

    public void setExpectedRate(int expectedRate) {
        this.expectedRate = expectedRate;
    }

    private Random rand = new Random(System.currentTimeMillis());

    public LoadGenerator() {
        if (sleepOverheadMicros == -1) {
            // calculate sleep overhead
            long totalSleepOverhead = 0;
            for (int i = 0; i < 50; i++) {
                long startTime = System.nanoTime();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ie) {
                }
                totalSleepOverhead += (System.nanoTime() - startTime)
                        - (1 * 1000 * 1000);
            }
            sleepOverheadMicros = (totalSleepOverhead / 50) / 1000;
        }
        System.out.println("Sleep overhead is " + sleepOverheadMicros);
    }

    public void run() {
        // for now, no warm-up mechanism
        adjustedExpectedRate = expectedRate;

        long startTime = 0;
        long intervalStart = 0;
        int emitCountStart = 0;
        long[] rateInfo = new long[2];
        rateInfo[0] = 100; // start with a sleep time of 100

        BufferedReader br = null;
        Reader inputReader = null;
        try {
            if (inputFilename.equals("-")) {
                inputReader = new InputStreamReader(System.in);
            } else {
                inputReader = new FileReader(inputFilename);
            }
            br = new BufferedReader(inputReader);
            String inputLine = null;
            boolean firstLine = true;
            EventWrapper eventWrapper = null;
            for (startTime = System.nanoTime(); (inputLine = br.readLine()) != null; startTime = System.nanoTime()) {
                if (firstLine) {
                    JSONObject jsonRecord = new JSONObject(inputLine);
                    createEventTypeInfo(jsonRecord);
                    System.out.println(eventTypeInfoMap);
                    if (eventTypeInfoMap.size() == 0) {
                        return;
                    }
                    firstLine = false;
                    continue;
                }

                try {
                    JSONObject jsonRecord = new JSONObject(inputLine);
                    int classIndex = jsonRecord.getInt("_index");
                    EventTypeInfo eventTypeInfo = eventTypeInfoMap.get(classIndex);
                    if (eventTypeInfo == null) {
                        System.err.printf("Invalid _index value %d\n",
                                          classIndex);
                        return;
                    }

                    Object event = makeRecord(jsonRecord,
                                              eventTypeInfo.getSchema());
                    eventWrapper = new EventWrapper(eventTypeInfo.getStreamName(),
                                                    event,
                                                    new ArrayList<CompoundKeyInfo>());
                    // System.out.println(eventWrapper.getStreamName() + ": " +
                    // eventWrapper.getEvent());
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.printf("Bad input data %s\n", inputLine);
                    continue;
                }

                int partition = Math.abs(rand.nextInt())
                        % eventEmitter.getNodeCount();

                eventEmitter.emit(partition, eventWrapper);
                emitCount++;

                // the rest of the stuff in this block is just to maintain the
                // rate
                processTimes[processTimePointer] = System.nanoTime()
                        - startTime;
                processTimePointer = (processTimePointer == PROCESS_TIME_LIST_MAX_SIZE - 1) ? 0
                        : processTimePointer + 1;
                if (emitCount == 1 || emitCount % 20 == 0) {
                    rateInfo = getRateInfo(rateInfo);
                }

                // if it's time, display the actual emit rate
                if (intervalStart == 0) {
                    intervalStart = System.currentTimeMillis();
                } else {
                    long interval = System.currentTimeMillis() - intervalStart;
                    if (interval >= (displayRateInterval * 1000)) {
                        double rate = (emitCount - emitCountStart)
                                / (interval / 1000.0);
                        System.out.println("Rate is " + rate);
                        intervalStart = System.currentTimeMillis();
                        emitCountStart = emitCount;
                    }
                }

                if (rateInfo[1] == 0 || emitCount % rateInfo[1] == 0) {
                    try {
                        Thread.sleep(rateInfo[0]);
                    } catch (InterruptedException ie) {
                    }
                }
            }
            System.out.printf("Emitted %d events\n", emitCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                br.close();
            } catch (Exception e) {
            }
            try {
                inputReader.close();
            } catch (Exception e) {
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void createEventTypeInfo(JSONObject classInfo) {
        String className = "";
        try {
            for (Iterator it = classInfo.keys(); it.hasNext();) {
                className = (String) it.next();
                JSONObject jsonEventTypeInfo = classInfo.getJSONObject(className);
                int classIndex = (Integer) jsonEventTypeInfo.getInt("classIndex");
                String streamName = jsonEventTypeInfo.getString("streamName");

                Class clazz = Class.forName(className);
                Schema schema = new Schema(clazz);
                eventTypeInfoMap.put(classIndex, new EventTypeInfo(schema,
                                                                   streamName));
            }
        } catch (JSONException je) {
            je.printStackTrace();
        } catch (ClassNotFoundException cnfe) {
            System.err.println("Count not locate class " + className);
        }
    }

    @SuppressWarnings("unchecked")
    private Object makeRecord(JSONObject jsonRecord, Schema schema) {
        Object event = null;
        try {
            event = schema.getType().newInstance();

            for (Iterator it = jsonRecord.keys(); it.hasNext();) {
                String propertyName = (String) it.next();

                Property property = schema.getProperties().get(propertyName);

                if (property == null) {
                    continue; // not in schema, just continue
                }

                Method setterMethod = property.getSetterMethod();
                Object value = jsonRecord.get(propertyName);
                if (value.equals(JSONObject.NULL)) {
                    continue;
                }

                setterMethod.invoke(event, makeSettableValue(property, value));

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return event;
    }

    @SuppressWarnings("unchecked")
    private Object makeSettableValue(Property property, Object value) {
        String propertyName = property.getName();
        Class propertyType = property.getType();

        if (propertyType.isArray()) {
            if (!(value instanceof JSONArray)) {
                System.err.println("Type mismatch for field " + propertyName);
                return null;
            }
            System.out.println("Is array!");
            return makeArray(property, (JSONArray) value);
        } else if (property.isList()) {
            if (!(value instanceof JSONArray)) {
                System.err.println("Type mismatch for field " + propertyName);
                return null;
            }
            return makeList(property, (JSONArray) value);
        } else if (propertyType.isPrimitive()) {
            if (!(value instanceof Number || value instanceof Boolean)) {
                System.err.println("Type mismatch for field " + propertyName
                        + "; expected number or boolean, found "
                        + value.getClass());
                return null;
            }
            return value; // hmm... does this work?
        } else if (propertyType.equals(String.class)) {
            if (!(value instanceof String)) {
                System.err.println("Type mismatch for field " + propertyName
                        + "; expected String, found " + value.getClass());
                return null;
            }
            return value;
        } else if (property.isNumber()) {
            if (!(value instanceof Integer || value instanceof Long
                    || value instanceof Float || value instanceof Double
                    || value instanceof BigDecimal || value instanceof BigInteger)) {
                return null;
            }

            Number adjustedValue = (Number) value;
            if (propertyType.equals(Long.class) && !(value instanceof Long)) {
                adjustedValue = new Long(((Number) value).longValue());
            } else if (propertyType.equals(Integer.class)
                    && !(value instanceof Integer)) {
                adjustedValue = new Integer(((Number) value).intValue());
            } else if (propertyType.equals(Double.class)
                    && !(value instanceof Double)) {
                adjustedValue = new Double(((Number) value).doubleValue());
            } else if (propertyType.equals(Float.class)
                    && !(value instanceof Float)) {
                adjustedValue = new Float(((Number) value).floatValue());
            } else if (propertyType.equals(BigDecimal.class)) {
                adjustedValue = new BigDecimal(((Number) value).longValue());
            } else if (propertyType.equals(BigInteger.class)) {
                adjustedValue = BigInteger.valueOf(((Number) value).longValue());
            }
            return adjustedValue;
        } else if (value instanceof JSONObject) {
            return makeRecord((JSONObject) value, property.getSchema());
        }

        return null;
    }

    public Object makeList(Property property, JSONArray jsonArray) {
        Property componentProperty = property.getComponentProperty();

        int size = jsonArray.length();

        List<Object> list = new ArrayList<Object>(size);

        try {
            for (int i = 0; i < size; i++) {
                Object value = jsonArray.get(i);
                list.add(makeSettableValue(componentProperty, value));
            }
        } catch (JSONException je) {
            throw new RuntimeException(je);
        }

        return list;
    }

    @SuppressWarnings("unchecked")
    public Object makeArray(Property property, JSONArray jsonArray) {
        Property componentProperty = property.getComponentProperty();
        Class clazz = componentProperty.getType();

        int size = jsonArray.length();

        Object array = Array.newInstance(clazz, size);

        try {
            for (int i = 0; i < size; i++) {
                Object value = jsonArray.get(i);
                Object adjustedValue = makeSettableValue(componentProperty,
                                                         value);
                Array.set(array, i, adjustedValue);
            }
        } catch (JSONException je) {
            throw new RuntimeException(je);
        }
        return array;
    }

    private long[] getRateInfo(long[] rateInfo) {
        long totalTimeNanos = 0;
        int entryCount = 0;
        for (int i = 0; i < processTimes.length; i++) {
            if (processTimes[i] == Long.MIN_VALUE) {
                break;
            }
            entryCount++;
            totalTimeNanos += processTimes[i];
        }
        long averageTimeMicros = (long) ((totalTimeNanos / (double) entryCount) / 1000.0);
        // fudge the time for additional overhead
        averageTimeMicros += (long) (averageTimeMicros * 0.30);

        if (emitCount % 5000 == 0) {
            // System.out.println("Average time in micros is " +
            // averageTimeMicros);
        }

        long sleepTimeMicros = 0;
        long millis = 0;

        long timeToMeetRateMicros = adjustedExpectedRate * averageTimeMicros;
        long leftOver = 1000000 - timeToMeetRateMicros;
        if (leftOver <= 0) {
            sleepTimeMicros = 0;
        } else {
            sleepTimeMicros = (leftOver / adjustedExpectedRate)
                    - sleepOverheadMicros;
        }

        // how many events can be processed in the nanos time?
        int eventsBeforeSleep = 1;
        if (sleepTimeMicros < 1000) {
            sleepTimeMicros = 1000 + sleepOverheadMicros;
            millis = 1;
            double numNapsDouble = ((double) leftOver / sleepTimeMicros);
            int numNaps = (int) Math.ceil(numNapsDouble);
            if (numNaps > 0) {
                eventsBeforeSleep = adjustedExpectedRate / numNaps;
            }

            if (leftOver <= 0) {
                millis = 0;
                eventsBeforeSleep = 1000;
            }
        } else {
            millis = sleepTimeMicros / 1000;
        }

        rateInfo[0] = millis;
        rateInfo[1] = eventsBeforeSleep;
        return rateInfo;
    }

    static class EventTypeInfo {
        private Schema schema;
        private String streamName;

        public EventTypeInfo(Schema schema, String streamName) {
            this.schema = schema;
            this.streamName = streamName;
        }

        public Schema getSchema() {
            return schema;
        }

        public String getStreamName() {
            return streamName;
        }

    }
}
