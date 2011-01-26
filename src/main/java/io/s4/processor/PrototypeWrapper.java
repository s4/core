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
import io.s4.persist.Persister;
import io.s4.util.clock.Clock;

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

    public PrototypeWrapper(ProcessingElement prototype, Clock s4Clock) {
        this.prototype = prototype;
        lookupTable = new ConMapPersister(s4Clock);
        System.out.println("Using ConMapPersister ..");
        // this bit of reflection is not a performance issue because it is only
        // invoked at configuration time
        try {
            ((ConMapPersister) lookupTable).setSelfClean(true);
            ((ConMapPersister) lookupTable).init();
            // set the persister in prototype
            Method method = prototype.getClass().getMethod("setLookupTable",
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

    /**
     * Find PE corresponding to keyValue. If no such PE exists, then a new one
     * is created by cloning the prototype and this is returned. As a
     * side-effect, the last update time for the PE in the lookup table is
     * modified.
     * 
     * @param keyValue
     *            key value
     * @return PE corresponding to keyValue.
     */
    public ProcessingElement getPE(String keyValue) {
        ProcessingElement pe = null;
        try {
            pe = (ProcessingElement) lookupTable.get(keyValue);
            if (pe == null) {
                pe = (ProcessingElement) prototype.clone();
                //invoke the initialization method if it has been specified
                if (pe.getInitMethod() != null) {
                   Method initMethod = pe.getClass().getMethod(pe.getInitMethod(), new Class[0]);
                   initMethod.invoke(pe, (new Object[0]));
                }

            }
            // update the last update time on the entry
            lookupTable.set(keyValue, pe, prototype.getTtl());

        } catch (Exception e) {
            logger.error("exception when looking up pe for key:" + keyValue, e);
        }

        return pe;
    }

    /**
     * Find PE corresponding to keyValue. If no such PE exists, then null is
     * returned. Note: the last update time is not modified in the lookup table.
     * 
     * @param keyValue
     *            key value
     * @return PE corresponding to keyValue, if such a PE exists. Null
     *         otherwise.
     */
    public ProcessingElement lookupPE(String keyValue) {
        ProcessingElement pe = null;

        try {
            pe = (ProcessingElement) lookupTable.get(keyValue);

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
