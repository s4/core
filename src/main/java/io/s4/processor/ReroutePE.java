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
import io.s4.dispatcher.Dispatcher;
import io.s4.dispatcher.partitioner.CompoundKeyInfo;
import io.s4.dispatcher.transformer.Transformer;
import io.s4.util.Cloner;
import io.s4.util.ClonerGenerator;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class ReroutePE extends AbstractPE {
    private static Logger logger = Logger.getLogger(ReroutePE.class);
    private Dispatcher dispatcher;
    private Transformer[] transformers = new Transformer[0];
    // private List<EventAdvice> keys;
    private String id = "ReroutePE";
    private String outputStreamName;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void setTransformers(Transformer[] transformers) {
        this.transformers = transformers;
    }

    // public void setEventNames(String[] eventNames) {
    // keys = new ArrayList<EventAdvice>();
    // for (String eventName : eventNames) {
    // keys.add(new EventAdvice(eventName, "*"));
    // }
    // }

    // @Override
    // public List<EventAdvice> advise() {
    // return keys;
    // }

    public void setOutputStreamName(String outputStreamName) {
        this.outputStreamName = outputStreamName;
    }

    private Map<String, Cloner> clonerMap = new HashMap<String, Cloner>();

    public void processEvent(Object event) {
        Object newEvent = event;
        if (transformers != null && transformers.length > 0) {
            Cloner cloner = clonerMap.get(event.getClass().getName());
            if (cloner == null) {
                ClonerGenerator cg = new ClonerGenerator();
                // generate byte code that knows how to call the clone method on
                // this event
                Class clonerClass = cg.generate(event.getClass());
                try {
                    cloner = (Cloner) clonerClass.newInstance();
                    clonerMap.put(event.getClass().getName(), cloner);
                } catch (InstantiationException ie) {
                    Logger.getLogger(this.getClass()).error(ie);
                    throw new RuntimeException(ie);
                } catch (IllegalAccessException ias) {
                    Logger.getLogger(this.getClass()).error(ias);
                    throw new RuntimeException(ias);
                }
            }
            newEvent = cloner.clone(event);
        }

        for (Transformer transformer : transformers) {
            newEvent = transformer.transform(newEvent);
            if (newEvent == null) {
                return;
            }
        }

        dispatcher.dispatchEvent(outputStreamName, newEvent);
    }

    public int getTtl() {
        return -1; // never die
    }

    @Override
    public void output() {
        // TODO Auto-generated method stub

    }

}
