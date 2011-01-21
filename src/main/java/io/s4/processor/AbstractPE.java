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

import io.s4.dispatcher.partitioner.CompoundKeyInfo;
import io.s4.dispatcher.partitioner.KeyInfo;
import io.s4.dispatcher.partitioner.KeyInfo.KeyPathElement;
import io.s4.dispatcher.partitioner.KeyInfo.KeyPathElementIndex;
import io.s4.dispatcher.partitioner.KeyInfo.KeyPathElementName;
import io.s4.persist.Persister;
import io.s4.schema.Schema;
import io.s4.schema.Schema.Property;
import io.s4.schema.SchemaContainer;
import io.s4.util.clock.Clock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

/**
 * This is the base class for processor classes. While it is possible to create
 * a processor class by implementing the {@link ProcessingElement} interface, we
 * suggest you instead extend this class.
 * <p>
 * <code>AbstractProcessor</code> provides output frequency strategies that
 * allow you to configure the rate at which your processor produces output (see
 * {@link AbstractPE#setOutputFrequencyByEventCount} and
 * {@link AbstractPE#setOutputFrequencyByTimeBoundary}.
 */
public abstract class AbstractPE implements ProcessingElement {
    public static enum FrequencyType {
        TIMEBOUNDARY("timeboundary"), EVENTCOUNT("eventcount");

        private String name;

        FrequencyType(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }

    private Clock s4Clock;
    private int outputFrequency = 1;
    private FrequencyType outputFrequencyType = FrequencyType.EVENTCOUNT;
    private int outputFrequencyOffset = 0;
    private int eventCount = 0;
    private int ttl = -1;
    private Persister lookupTable;
    private List<EventAdvice> eventAdviceList = new ArrayList<EventAdvice>();
    private List<Object> keyValue;
    private List<Object> keyRecord;
    private String keyValueString;
    private String streamName;
    private boolean saveKeyRecord = false;
    private int outputsBeforePause = -1;
    private long pauseTimeInMillis;
    private boolean logPauses = false;

    public void setSaveKeyRecord(boolean saveKeyRecord) {
        this.saveKeyRecord = saveKeyRecord;
    }

    public void setOutputsBeforePause(int outputsBeforePause) {
        this.outputsBeforePause = outputsBeforePause;
    }

    public void setPauseTimeInMillis(long pauseTimeInMillis) {
        this.pauseTimeInMillis = pauseTimeInMillis;
    }

    public void setLogPauses(boolean logPauses) {
        this.logPauses = logPauses;
    }

    public void setS4Clock(Clock s4Clock) {
        synchronized (this) {
            this.s4Clock = s4Clock;
            this.notify();
        }
    }

    public Clock getS4Clock() {
        return s4Clock;
    }

    private OverloadDispatcher overloadDispatcher;

    public AbstractPE() {
        OverloadDispatcherGenerator oldg = new OverloadDispatcherGenerator(this.getClass());
        Class<?> overloadDispatcherClass = oldg.generate();

        try {
            overloadDispatcher = (OverloadDispatcher) overloadDispatcherClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This implements the <code>execute</code> method declared in the
     * {@link ProcessingElement} interface. You should not override this method.
     * Instead, you need to implement the <code>processEvent</code> method.
     **/
    public void execute(String streamName, CompoundKeyInfo compoundKeyInfo,
                        Object event) {
        // if this is the first time through, get the key for this PE
        if (keyValue == null || saveKeyRecord) {
            setKeyValue(event, compoundKeyInfo);

            if (compoundKeyInfo != null)
                keyValueString = compoundKeyInfo.getCompoundValue();
        }

        this.streamName = streamName;

        overloadDispatcher.dispatch(this, event);

        if (saveKeyRecord) {
            keyRecord.clear(); // the PE doesn't need it anymore
        }

        if (outputFrequencyType == FrequencyType.EVENTCOUNT
                && outputFrequency > 0) {
            eventCount++;
            if (eventCount % outputFrequency == 0) {
                try {
                    output();
                } catch (Exception e) {
                    Logger.getLogger("s4")
                          .error("Exception calling output() method in execute()",
                                 e);
                }
            }
        }
    }

    public long getCurrentTime() {
        return s4Clock.getCurrentTime();
    }

    /**
     * This method returns the key value associated with this PE.
     * <p>
     * The key value is a list because the key may be a compound (composite)
     * key, in which case the key will have one value for each simple key.
     * 
     * @return the key value as a List of Objects (each element contains the
     *         value of a simple key).
     **/
    public List<Object> getKeyValue() {
        return keyValue;
    }

    public List<Object> getKeyRecord() {
        return keyRecord;
    }

    public String getKeyValueString() {
        return keyValueString;
    }

    public String getStreamName() {
        return streamName;
    }

    SchemaContainer schemaContainer = new SchemaContainer();

    private void setKeyValue(Object event, CompoundKeyInfo compoundKeyInfo) {
        if (compoundKeyInfo == null) {
            return;
        }

        keyValue = new ArrayList<Object>();

        Schema schema = schemaContainer.getSchema(event.getClass());

        // get the value for each keyInfo
        for (KeyInfo keyInfo : compoundKeyInfo.getKeyInfoList()) {
            Object value = null;
            Object record = event;
            List<?> list = null;
            Property property = null;
            for (KeyPathElement keyPathElement : keyInfo.getKeyPath()) {
                if (keyPathElement instanceof KeyPathElementIndex) {
                    record = list.get(((KeyPathElementIndex) keyPathElement).getIndex());
                    schema = property.getComponentProperty().getSchema();
                } else {
                    String keyPathElementName = ((KeyPathElementName) keyPathElement).getKeyName();
                    property = schema.getProperties().get(keyPathElementName);
                    value = null;
                    try {
                        value = property.getGetterMethod().invoke(record);
                    } catch (Exception e) {
                        Logger.getLogger("s4").error(e);
                        return;
                    }

                    if (value == null) {
                        Logger.getLogger("s4").error("Value for "
                                + keyPathElementName + " is null!");
                        return;
                    }

                    if (property.getType().isPrimitive() || property.isNumber()
                            || property.getType().equals(String.class)) {
                        keyValue.add(value);
                        if (saveKeyRecord) {
                            if (keyRecord == null) {
                                keyRecord = new ArrayList<Object>();
                            }
                            keyRecord.add(record);
                        }
                        continue;
                    } else if (property.isList()) {
                        try {
                            list = (List) property.getGetterMethod()
                                                  .invoke(record);
                        } catch (Exception e) {
                            Logger.getLogger("s4").error(e);
                            return;
                        }
                    } else {
                        try {
                            record = property.getGetterMethod().invoke(record);
                        } catch (Exception e) {
                            Logger.getLogger("s4").error(e);
                            return;
                        }
                        schema = property.getSchema();
                    }
                }
            }
        }
    }

    /**
     * This method sets the output strategy to "by event count" and specifies
     * how many events trigger a call to the <code>output</code> method.
     * <p>
     * You would not normally call this method directly, but instead via the S4
     * configuration file.
     * <p>
     * After this method is called, AbstractProcessor will call your
     * <code>output</code> method (implemented in your subclass) every
     * <emp>outputFrequency</emph> events.
     * <p>
     * If you call neither <code>setOutputFrequencyByEventCount</code> nor
     * <code>setOutputFrequencyByTimeBoundary</code>, the default strategy is
     * "by event count" with an output frequency of 1. (That is,
     * <code>output</code> is called after after each return from
     * <code>processEvent</code>).
     * 
     * @param outputFrequency
     *            the number of events passed to <code>processEvent</code>
     *            before output is called.
     **/
    public void setOutputFrequencyByEventCount(int outputFrequency) {
        this.outputFrequency = outputFrequency;
        this.outputFrequencyType = FrequencyType.EVENTCOUNT;
        initFrequency();
    }

    /**
     * This method sets the output strategy to "output on time boundary" and
     * specifies the time boundary on which the <code>output</code> should be
     * called.
     * <p>
     * You would not normally call this method directly, but instead via the S4
     * configuration file.
     * <p>
     * <code>outputFrequency</code> specifies the time boundary in seconds.
     * Whenever the current time is a multiple of <code>outputFrequency</code>,
     * <code>AbstractProcessor</code> will call your <code>output</code> method.
     * For example, if you specify an <code>outputFrequency</code> of 3600,
     * <code>AbstractProcessor</code> will call <code>output</code> on every
     * hour boundary (e.g., 11:00:00, 12:00:00, 13:00:00, etc.).
     * <p>
     * When this output strategy is used, your <code>output</code> method may
     * occasionally (or frequently) run concurrently with your
     * <code>processEvent</code> method. Therefore, you should take steps to
     * protect any data structures that both methods use.
     * <p>
     * If you call neither <code>setOutputFrequencyByEventCount</code> nor
     * <code>setOutputFrequencyByTimeBoundary</code>, the default strategy is
     * "by event count" with an output frequency of 1. (That is,
     * <code>output</code> is called after after each return from
     * <code>processEvent</code>).
     * 
     * @param outputFrequency
     *            the time boundary in seconds
     **/
    public void setOutputFrequencyByTimeBoundary(int outputFrequency) {
        this.outputFrequency = outputFrequency;
        this.outputFrequencyType = FrequencyType.TIMEBOUNDARY;
        initFrequency();
    }

    /**
     * Set the offset from the time boundary at which
     * <code>AbstractProcessor</code> should call <code>output</code>.
     * <p>
     * This value is honored only if the "output on time boundary" output
     * strategy is used.
     * <p>
     * As an example, if you specify an <code>outputFrequency</code> of 3600 and
     * an <code>outputFrequencyOffset</code> of 7,
     * <code>AbstractProcessor</code> will call <code>output</code> on every
     * hour boundary plus 7 seconds (e.g., 11:00:07, 12:00:07, 13:00:07, etc.).
     **/
    public void setOutputFrequencyOffset(int outputFrequencyOffset) {
        this.outputFrequencyOffset = outputFrequencyOffset;
    }

    public void setKeys(String[] keys) {
        for (String key : keys) {
            StringTokenizer st = new StringTokenizer(key);
            eventAdviceList.add(new EventAdvice(st.nextToken(), st.nextToken()));
        }
    }

    private void initFrequency() {
        if (outputFrequency < 0) {
            return;
        }

        if (outputFrequencyType == FrequencyType.TIMEBOUNDARY) {
            // create a thread that calls output on time boundaries
            // that are multiples of frequency
            Runnable r = new OutputInvoker();

            Thread t = new Thread(r);
            t.start();
        }
    }

    /**
     * This method exists simply to make <code>clone()</code> public.
     */
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    /**
     * 
     */
    public int getTtl() {
        return ttl;
    }

    public List<EventAdvice> advise() {
        return eventAdviceList;
    }

    /**
     * 
     */
    public void setLookupTable(Persister lookupTable) {
        this.lookupTable = lookupTable;
    }

    /**
     * You implement this abstract method in your subclass. This is the part of
     * your processor that outputs data (e.g., by writing the data to the
     * cache). The <code>output</code> method may further process the data
     * (e.g., aggregate it) before outputting it.
     **/
    abstract public void output();

    class OutputInvoker implements Runnable {
        public void run() {
            synchronized (AbstractPE.this) {
                while (s4Clock == null) {
                    try {
                        AbstractPE.this.wait();
                    } catch (InterruptedException ie) {
                    }
                }
            }
            int outputCount = 0;
            long frequencyInMillis = outputFrequency * 1000;

            long currentTime = getCurrentTime();
            while (!Thread.interrupted()) {
                long currentBoundary = (currentTime / frequencyInMillis)
                        * frequencyInMillis;
                long nextBoundary = currentBoundary + frequencyInMillis;
                currentTime = s4Clock.waitForTime(nextBoundary
                        + (outputFrequencyOffset * 1000));
                if (lookupTable != null) {
                    Set peKeys = lookupTable.keySet();
                    for (Iterator it = peKeys.iterator(); it.hasNext();) {
                        String peKey = (String) it.next();
                        AbstractPE pe = null;
                        try {
                            pe = (AbstractPE) lookupTable.get(peKey);
                        } catch (InterruptedException ie) {
                        }

                        if (pe == null) {
                            continue;
                        }

                        try {
                            pe.output();
                            outputCount++;
                        } catch (Exception e) {
                            Logger.getLogger("s4")
                                  .error("Exception calling output() method", e);
                        }

                        if (outputCount == outputsBeforePause) {
                            if (logPauses) {
                                Logger.getLogger("s4").info("Pausing "
                                        + getId() + " at count " + outputCount
                                        + " for " + pauseTimeInMillis
                                        + " milliseconds");
                            }
                            outputCount = 0;
                            try {
                                Thread.sleep(pauseTimeInMillis);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    } // end for each pe in lookup table
                } // end if lookup table is not null
            }
        }
    }
}
