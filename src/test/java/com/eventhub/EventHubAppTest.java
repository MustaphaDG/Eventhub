package com.eventhub;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test for EventHubApp.
 */
public class EventHubAppTest {
    /**
     * Sanity check for class loading.
     */
    @Test
    public void shouldLoadClass() {
        assertNotNull(EventHubApp.class);
    }

    @Test
    public void shouldDetectEntityPath() {
        assertTrue(EventHubApp.hasEntityPath("Endpoint=sb://test/;SharedAccessKeyName=name;SharedAccessKey=key;EntityPath=hub"));
        assertTrue(EventHubApp.hasEntityPath("EntityPath=hub;Endpoint=sb://test/;SharedAccessKeyName=name;SharedAccessKey=key"));
        assertFalse(EventHubApp.hasEntityPath("Endpoint=sb://test/;SharedAccessKeyName=name;SharedAccessKey=key"));
    }
}
