package io.s4.util;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DrivenClock {
    private volatile long currentTime;
    private NavigableMap<Long, List<TimerRequest>> timerRequests = new TreeMap<Long, List<TimerRequest>>();

    public void tick(long newCurrentTime) {
        if (newCurrentTime < currentTime) {
            return;
        }
        // race condition here, but it's OK
        currentTime = newCurrentTime;

        while (true) {
            // inspect the top of the timer request list and see if any request
            // is
            // satisfied by the new current time
            List<TimerRequest> relevantRequests = null;
            synchronized (timerRequests) {
                Entry<Long, List<TimerRequest>> entry = timerRequests.firstEntry();
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
        if (targetTime <= currentTime) {
            return currentTime;
        }

        TimerRequest timerRequest = new TimerRequest(targetTime);
        synchronized (timerRequests) {
            List<TimerRequest> requestsForTargetTime = timerRequests.get(targetTime);
            if (requestsForTargetTime == null) {
                requestsForTargetTime = new ArrayList<TimerRequest>();
                timerRequests.put(targetTime, requestsForTargetTime);
            }
            requestsForTargetTime.add(timerRequest);
        }

        return timerRequest.waitForTargetTime();
    }

    public static class TimerRequest {
        private long targetTime;
        private BlockingQueue<Long> blockingQueue = new LinkedBlockingQueue<Long>();

        public TimerRequest(long targetTime) {
            this.targetTime = targetTime;
        }

        public long getTargetTime() {
            return targetTime;
        }

        public void wakeUp(long currentTime) {
            blockingQueue.add(currentTime);
        }

        public long waitForTargetTime() {
            try {
                return blockingQueue.take();
            } catch (InterruptedException ie) {
                return -1;
            }
        }

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
