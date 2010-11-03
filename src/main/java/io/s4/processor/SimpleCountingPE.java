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
import io.s4.persist.Persister;

import java.util.List;

public class SimpleCountingPE extends AbstractPE {
    private boolean clearOnOutput;
    private OutputFormatter outputFormatter;
    private Persister persister;
    private int persistTime;
    private String keyPrefix = "s4:counter";
    private boolean dirty = false;
    private String id = "SimpleCountingPE";

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setClearOnOutput(boolean clearOnOutput) {
        this.clearOnOutput = clearOnOutput;
    }

    public void setOutputFormatter(OutputFormatter outputFormatter) {
        this.outputFormatter = outputFormatter;
    }

    public void setPersister(Persister persister) {
        this.persister = persister;
    }

    public void setPersistTime(int persistTime) {
        this.persistTime = persistTime;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    private long counter = 0;

    @Override
    public void output() {
        Object outputValue = null;
        synchronized (this) {
            if (!dirty) {
                return;
            }
            outputValue = new Long(counter);
            if (clearOnOutput) {
                counter = 0;
            }
            dirty = false;
        }

        List<Object> simpleKeyValues = this.getKeyValue();
        StringBuffer keyBuffer = new StringBuffer(keyPrefix);
        for (Object simpleKeyValue : simpleKeyValues) {
            keyBuffer.append(":");
            keyBuffer.append(String.valueOf(simpleKeyValue));
        }

        if (outputFormatter != null) {
            outputValue = outputFormatter.format(outputValue);
        }

        try {
            persister.set(keyBuffer.toString(), outputValue, persistTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

    }

    public void processEvent(Object event) {
        synchronized (this) {
            counter++;
            dirty = true;
        }
    }

}
