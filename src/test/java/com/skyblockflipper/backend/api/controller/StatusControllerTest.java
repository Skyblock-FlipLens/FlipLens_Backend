package com.skyblockflipper.backend.api.controller;

import com.skyblockflipper.backend.api.dto.*;

import com.skyblockflipper.backend.hypixel.HypixelClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class StatusControllerTest {

    @Test
    void statusReturnsOkAndTriggersFetch() {
        HypixelClient hypixelClient = mock(HypixelClient.class);
        StatusController controller = new StatusController(hypixelClient);

        StatusResponse response = controller.status();

        assertEquals("ok", response.status());
        verify(hypixelClient).fetchAuctions();
    }
}


