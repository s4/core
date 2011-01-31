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
package io.s4.dispatcher;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class StreamSelectingDispatcher implements EventDispatcher {

    private EventDispatcher dispatcher = null;

    public void setDispatcher(EventDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    private HashSet<String> streams = null;

    public void setStreams(String[] streams) {
        this.streams = new HashSet<String>(Arrays.asList(streams));
    }

    @Override
    public void dispatchEvent(String streamName, Object event) {
        if (dispatcher != null && streams != null
                && streams.contains(streamName)) {
            dispatcher.dispatchEvent(streamName, event);
        }
    }

    @Override
    public void dispatchEvent(String streamName,
                              List<List<String>> compoundKeyNames, Object event) {
        if (dispatcher != null && streams != null
                && streams.contains(streamName)) {
            dispatcher.dispatchEvent(streamName, compoundKeyNames, event);
        }
    }
}
