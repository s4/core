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

package io.s4.util;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

public class DrivenClock implements Clock {
    
    private volatile long currentTime;
    private NavigableMap<Long, List<TimerRequest>> timerRequests = new TreeMap<Long, List<TimerRequest>>();

    public void updateTime(long newCurrentTime) {
        if (newCurrentTime < currentTime) {
            return;
        }
        List<TimerRequest> relevantRequests = null;
        synchronized (timerRequests) {
            currentTime = newCurrentTime;
            while (true) {
                // inspect the top of the timer request list and see if any
                // request
                // is
                // satisfied by the new current time
                Entry<Long, List<TimerRequest>> entry = timerRequests
                        .firstEntry();
                if (entry == null || entry.getKey() > newCurrentTime) {
                    break;
                }
                relevantRequests = timerRequests.remove(entry.getKey());
            }
            if (relevantRequests != null) {
                for (TimerRequest timerRequest : relevantRequests) {
                    timerRequest.wakeUp(newCurrentTime);
                }
            }
        }
    }

    public long waitForTime(long targetTime) {
        TimerRequest timerRequest = null;
        synchronized (timerRequests) {
            if (targetTime <= currentTime) {
                return currentTime;
            }
            timerRequest = new TimerRequest(targetTime);
            List<TimerRequest> requestsForTargetTime = timerRequests.get(targetTime);
            if (requestsForTargetTime == null) {
                requestsForTargetTime = new ArrayList<TimerRequest>();
                timerRequests.put(targetTime, requestsForTargetTime);
            }
            requestsForTargetTime.add(timerRequest);
        }
        return timerRequest.waitForTargetTime();
    }


    public long getCurrentTime() {
        return getCurrentTime(true);
    }

    public long getCurrentTime(boolean waitOnInitialization) {
        if (currentTime == 0 && waitOnInitialization) {
            // if tick has never been called, wait for it to be called once
            this.waitForTime(1);
        }
        return currentTime;
    }
}
