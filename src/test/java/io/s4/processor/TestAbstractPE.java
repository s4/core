package io.s4.processor;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Verify AbstractPE
 * TODO more unit tests 
 */
public class TestAbstractPE {

    
    /**
     * Verifies ability to set an initialize method that will be called when 
     * a new PE is instantiated
     */
    @Test
    public void testCloneAndInitialize() {
        MockPE prototype = new MockPE();
        prototype.setInitMethod("testInitialize");

        assertEquals(0, prototype.getInitializeCount());
        MockPE instance = (MockPE) prototype.clone();
        
        assertEquals(0, prototype.getInitializeCount());
        assertEquals(1, instance.getInitializeCount());
    }
}
