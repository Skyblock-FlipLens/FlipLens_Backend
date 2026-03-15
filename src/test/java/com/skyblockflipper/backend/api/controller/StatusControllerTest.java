package com.skyblockflipper.backend.api.controller;

import com.skyblockflipper.backend.api.dto.StatusResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StatusControllerTest {

    @Test
    void statusReturnsOk() {
        StatusController controller = new StatusController();

        StatusResponse response = controller.status();

        assertEquals("ok", response.status());
    }
}


