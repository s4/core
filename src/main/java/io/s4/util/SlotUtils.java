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


public class SlotUtils {

    private int slotSize; // slot size in seconds

    public SlotUtils(int slotSize) {
        this.slotSize = slotSize;
    }

    public void setSize(int slotSize) {
        this.slotSize = slotSize;
    }

    public Long getSlotAtTime(long time) {
        return slotSize * (long) Math.floor(time / slotSize);
    }

    public Long getCurrentSlot() {
        long currTimeStamp = System.currentTimeMillis() / 1000; // convert to
        // seconds
        Long slotTimeStamp = getSlotAtTime(currTimeStamp);
        return slotTimeStamp;
    }

    public Long getSlot(int index, long currTimeStamp) {
        Long slotTimeStamp = getSlotAtTime(currTimeStamp + index * slotSize);
        return slotTimeStamp;
    }

    public boolean isOutsideWindow(Long slot, int windowSize, long time) {
        boolean outside = false;
        long windowBoundary = getSlotAtTime(time) - windowSize;
        if (slot.longValue() < windowBoundary) {
            outside = true;
        }
        return outside;
    }

    public static void main(String[] args) {
        SlotUtils s = new SlotUtils(300);
        System.out.printf("%d\n", s.getCurrentSlot());
    }
}
