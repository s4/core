package io.s4.processor;

/**
 * Mock PE for asserting correct behavior of AbstractPE
 */
public class MockPE extends AbstractPE {

    private int initializeCount = 0;
    
    public void testInitialize() {
        initializeCount++;
    }
    
    public void processEvent(Object obj) {
    }
    
    @Override
    public String getId() {
        return null;
    }

    @Override
    public void output() {

    }

    /**
     * @return the initializeCount
     */
    public int getInitializeCount() {
        return initializeCount;
    }

}
