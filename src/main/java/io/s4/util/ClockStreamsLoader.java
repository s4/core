/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *          http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */

package io.s4.util;

import java.util.HashMap;

import org.apache.log4j.Logger;

public class ClockStreamsLoader {

    private static Logger logger = Logger.getLogger(ClockStreamsLoader.class);
    Clock s4Clock;
    HashMap<String, String> streamFieldMap;

    public void setStreamFieldMap(HashMap<String, String> streamFieldMap) {
        this.streamFieldMap = streamFieldMap;
    }

    public void setS4Clock(Clock s4Clock) {
        this.s4Clock = s4Clock;
    }

    public void addStreams() {
        System.out.println("Adding application streams to s4 clock");
        if (s4Clock instanceof EventClock) {
            EventClock eventClock = (EventClock) s4Clock;
            for (String streamName : streamFieldMap.keySet()) {
                eventClock.addEventClockStream(streamName,
                        streamFieldMap.get(streamName));
            }
        }
    }
}
