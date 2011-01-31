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

import io.s4.collector.EventWrapper;
import io.s4.dispatcher.EventDispatcher;
import io.s4.dispatcher.partitioner.CompoundKeyInfo;
import io.s4.message.PrototypeRequest;
import io.s4.message.Response;
import io.s4.message.SinglePERequest;

import java.util.List;

/**
 * Processes control events.
 */
public class ControlEventProcessor {

    private EventDispatcher dispatcher;

    public void setDispatcher(EventDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void process(EventWrapper e, PrototypeWrapper p) {
        String en = e.getStreamName(); // e.g. "#joinPe01"
        String pn = p.getId(); // e.g. "JoinPE01"

        // stream name has to match PE's ID (modulo case).
        // e.g. "#joinPe01" will match "JoinPE01"
        if (!en.regionMatches(true, 1, pn, 0, pn.length()))
            return;

        execute(e, p);
    }

    protected void execute(EventWrapper e, PrototypeWrapper p) {
        List<CompoundKeyInfo> keyInfoList = e.getCompoundKeys();
        Object event = e.getEvent();
        
        if (event instanceof SinglePERequest) {
            // Handle Requests to individual PEs
            if (keyInfoList.isEmpty())
                return;

            CompoundKeyInfo keyInfo = keyInfoList.get(0);

            String keyVal = keyInfo.getCompoundValue();

            ProcessingElement pe = p.lookupPE(keyVal);

            Response response = ((SinglePERequest) event).evaluate(pe);
            String stream = response.getRInfo().getStream();

            dispatcher.dispatchEvent(stream, response);

        } else if (event instanceof PrototypeRequest) {
            // Or handle aggregate requests to Prototypes.
            Response response = ((PrototypeRequest) event).evaluate(p);
            String stream = response.getRInfo().getStream();

            dispatcher.dispatchEvent(stream, response);
        }

    }
}
