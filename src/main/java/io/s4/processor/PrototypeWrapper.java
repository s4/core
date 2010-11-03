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

import io.s4.persist.ConMapPersister;
import io.s4.persist.HashMapPersister;
import io.s4.persist.Persister;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.log4j.Logger;

public class PrototypeWrapper {

    private static Logger logger = Logger.getLogger(PrototypeWrapper.class);
    private ProcessingElement prototype;
    Persister lookupTable;

    public String getId() {
        return prototype.getId();
    }

    public PrototypeWrapper(ProcessingElement prototype) {
        this.prototype = prototype;
        // lookupTable = new HashMapPersister();
        lookupTable = new ConMapPersister();
        System.out.println("Using ConMapPersister ..");
        // this bit of reflection is not a performance issue because it is only
        // invoked at configuration time
        try {
            // lookupTable.setSelfClean(true);
            // lookupTable.init();
            Method method;
            method = lookupTable.getClass().getMethod("setSelfClean",
                                                      boolean.class);
            method.invoke(lookupTable, true);
            method = lookupTable.getClass().getMethod("init");
            method.invoke(lookupTable);
            // set the persister in prototype
            method = prototype.getClass().getMethod("setLookupTable",
                                                    Persister.class);
            method.invoke(prototype, lookupTable);
        } catch (NoSuchMethodException e) {
            // this is expected
        } catch (Exception e) {
            // this is not expected
            Logger.getLogger("s4")
                  .error("Exception invoking setLookupTable on prototype", e);
        }
    }

    public ProcessingElement getPE(String keyValue) {
        ProcessingElement pe = null;
        try {
            pe = (ProcessingElement) lookupTable.get(keyValue);
            if (pe == null) {
                pe = (ProcessingElement) prototype.clone();
            }
            // update the last update time on the entry
            lookupTable.set(keyValue, pe, prototype.getTtl());

        } catch (Exception e) {
            logger.error("exception when looking up pe for key:" + keyValue, e);
        }

        return pe;
    }

    public int getPECount() {
        return lookupTable.keySet().size();
    }

    public List<EventAdvice> advise() {
        return prototype.advise();
    }
}
