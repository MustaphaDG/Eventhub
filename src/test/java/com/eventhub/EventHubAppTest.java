package com.eventhub;

import static org.junit.Assert.assertNotNull;

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
}
