package io.s4.test;

import io.s4.persist.ConMapPersister;
import io.s4.persist.HashMapPersister;
import io.s4.persist.Persister;
import io.s4.util.EventClock;

public class TestPersisterEventClock {

    static EventClock s4Clock;
    static Persister persister;  
    
    public static void main(String[] args) {
        TestPersisterEventClock testPersisterClock = new TestPersisterEventClock();
        s4Clock = new EventClock();
        s4Clock.updateTime(69990000);
        TimeUpdaterThread timeUpdater  = new TimeUpdaterThread(s4Clock);
        Thread timeUpdaterThread = new Thread(timeUpdater);
        timeUpdaterThread.start();
        persister = new HashMapPersister(s4Clock);
        testPersisterClock.testPersister(persister);
        persister = new ConMapPersister(s4Clock);
        testPersisterClock.testPersister(persister);
    }
    
    public void testPersister(Persister persister) {
        HashMapPersister hp = null;
        ConMapPersister cp = null;
        if (persister instanceof HashMapPersister) {
            hp = (HashMapPersister)persister;
            hp.init();

            hp.set("mykey1", "Test1", 40);
            hp.set("mykey2", "Test2", 48);
            hp.set("mykey3", "Test2", -1);

            try {
                s4Clock.waitForTime(s4Clock.getCurrentTime() + 1);
            } catch (Exception e) {
            }

            System.out.println("mykey1: " + hp.get("mykey1"));
            System.out.println("mykey2: " + hp.get("mykey2"));
            System.out.println("mykey3: " + hp.get("mykey3"));

            System.out.println("Going to sleep...");
            try {
                s4Clock.waitForTime(s4Clock.getCurrentTime() + 41000);
            } catch (Exception e) {
            }
            System.out.println("Waking up");

            System.out.println("mykey1: " + hp.get("mykey1"));
            System.out.println("mykey2: " + hp.get("mykey2"));
            System.out.println("mykey3: " + hp.get("mykey3"));

            System.out.println("Going to sleep...");
            try {
                s4Clock.waitForTime(s4Clock.getCurrentTime() + 10000);
            } catch (Exception e) {
            }
            System.out.println("Waking up");

            System.out.println("mykey1: " + hp.get("mykey1"));
            System.out.println("mykey2: " + hp.get("mykey2"));
            System.out.println("mykey3: " + hp.get("mykey3"));
            System.out.println("cleanUp: " + hp.cleanOutGarbage());

        }
        if (persister instanceof ConMapPersister) {
            cp = (ConMapPersister)persister;
            cp.init();

            cp.set("mykey1", "Test1", 40);
            cp.set("mykey2", "Test2", 48);
            cp.set("mykey3", "Test2", -1);

            try {
                s4Clock.waitForTime(s4Clock.getCurrentTime() + 1);
            } catch (Exception e) {
            }

            System.out.println("mykey1: " + cp.get("mykey1"));
            System.out.println("mykey2: " + cp.get("mykey2"));
            System.out.println("mykey3: " + cp.get("mykey3"));

            System.out.println("Going to sleep...");
            try {
                s4Clock.waitForTime(s4Clock.getCurrentTime() + 41000);
            } catch (Exception e) {
            }
            System.out.println("Waking up");

            System.out.println("mykey1: " + cp.get("mykey1"));
            System.out.println("mykey2: " + cp.get("mykey2"));
            System.out.println("mykey3: " + cp.get("mykey3"));

            System.out.println("Going to sleep...");
            try {
                s4Clock.waitForTime(s4Clock.getCurrentTime() + 10000);
            } catch (Exception e) {
            }
            System.out.println("Waking up");

            System.out.println("mykey1: " + cp.get("mykey1"));
            System.out.println("mykey2: " + cp.get("mykey2"));
            System.out.println("mykey3: " + cp.get("mykey3"));
            System.out.println("cleanUp: " + cp.cleanOutGarbage());
        }
    }
    

    
}
