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
package io.s4.test;

import io.s4.util.EventClock;

public class TimeUpdaterThread implements Runnable {

    EventClock s4Clock;

    public TimeUpdaterThread(EventClock s4Clock) {
        this.s4Clock = s4Clock;
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub

        // tick the clock
        for (long currentTime = 69996400; currentTime <= 70099900; currentTime += 400) {
            System.out.println("Setting time to " + currentTime);
            s4Clock.updateTime(currentTime);
            try {
                Thread.sleep(400);
            } catch (InterruptedException ie) {
            }
        }
    }

}