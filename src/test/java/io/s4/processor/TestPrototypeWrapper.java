package io.s4.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import io.s4.util.clock.WallClock;

import org.junit.Test;

public class TestPrototypeWrapper
{
   
   /**
    * Verifies ability to set an initialize method that will be called when 
    * a new PE is instantiated
    */
   @Test
   public void testCloneAndInitialize() {
       MockPE prototype = new MockPE();
       prototype.setInitMethod("testInitialize");
       
       PrototypeWrapper prototypeWrapper = new PrototypeWrapper(prototype, new WallClock());

       assertEquals(0, prototype.getInitializeCount());
       MockPE instance = (MockPE)prototypeWrapper.getPE("asd");
       assertNotNull(instance);
       
       assertEquals(0, prototype.getInitializeCount());
       assertEquals(1, instance.getInitializeCount());
   }
   
}
