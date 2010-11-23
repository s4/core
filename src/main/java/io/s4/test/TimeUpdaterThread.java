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
            }
            catch (InterruptedException ie) {}  
        }            
    }
    
} 