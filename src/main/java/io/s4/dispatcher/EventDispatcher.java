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

public interface EventDispatcher {

    /**
     * Dispatch event using stream name. Partitioners may be used to partition
     * the event, possibly based on a pre-determined set of fixed named keys.
     * 
     * @param streamName
     *            name of stream to dispatch on
     * @param event
     *            object to dispatch
     */
    void dispatchEvent(String streamName, Object event);

    /**
     * Dispatch event using a stream name and using a set of named keys.
     * VariableKeyPartitioners may be used to partition the event.
     * 
     * @param streamName
     *            name of stream to dispatch on
     * @param compoundKeyNames
     *            keys to use for dispatching
     * @param event
     *            object to dispatch
     */
    void dispatchEvent(String streamName, List<List<String>> compoundKeyNames,
                       Object event);

}