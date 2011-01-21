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

import java.util.List;

/**
 * Dispatcher that sends events through multiple abstract dispatchers.
 */
public class MultiDispatcher implements EventDispatcher {

    private EventDispatcher[] dispatchers = null;

    public void setDispatchers(EventDispatcher[] dispatchers) {
        this.dispatchers = dispatchers;
    }

    @Override
    public void dispatchEvent(String streamName, Object event) {
        if (dispatchers != null) {
            System.out.println("Dispatching on stream " + streamName
                    + " event: " + event);
            for (EventDispatcher dispatcher : dispatchers) {
                System.out.println("trying " + dispatcher);
                dispatcher.dispatchEvent(streamName, event);
            }
        }
    }

    @Override
    public void dispatchEvent(String streamName,
                              List<List<String>> compoundKeyNames, Object event) {
        if (dispatchers != null) {
            for (EventDispatcher dispatcher : dispatchers) {
                dispatcher.dispatchEvent(streamName, compoundKeyNames, event);
            }
        }
    }
}
