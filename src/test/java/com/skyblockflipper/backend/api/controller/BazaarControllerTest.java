package com.skyblockflipper.backend.api.controller;

import com.skyblockflipper.backend.api.dto.*;

import com.skyblockflipper.backend.service.market.BazaarReadService;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BazaarControllerTest {

    @Test
    void productReturnsOkWhenFoundAndNotFoundOtherwise() {
        BazaarReadService service = mock(BazaarReadService.class);
        BazaarController controller = new BazaarController(service);
        BazaarProductDto dto = new BazaarProductDto("A", 100, 90, 10, 10, 1, 1, 100, 100);

        when(service.getProduct("A")).thenReturn(Optional.of(dto));
        when(service.getProduct("MISSING")).thenReturn(Optional.empty());

        ResponseEntity<BazaarProductDto> found = controller.product("A");
        ResponseEntity<BazaarProductDto> missing = controller.product("MISSING");

        assertEquals(HttpStatus.OK, found.getStatusCode());
        assertEquals(dto, found.getBody());
        assertEquals(HttpStatus.NOT_FOUND, missing.getStatusCode());
    }

    @Test
    void orderBookDelegatesToService() {
        BazaarReadService service = mock(BazaarReadService.class);
        BazaarController controller = new BazaarController(service);
        BazaarOrderBookDto expected = new BazaarOrderBookDto(
                List.of(new BazaarOrderBookDto.OrderLevelDto(101.0, 5, 1)),
                List.of(new BazaarOrderBookDto.OrderLevelDto(99.0, 10, 2))
        );
        when(service.getOrderBook("A", 15)).thenReturn(expected);

        BazaarOrderBookDto response = controller.orderBook("A", 15);

        assertEquals(expected, response);
        verify(service).getOrderBook("A", 15);
    }

    @Test
    void quickFlipsDelegatesToService() {
        BazaarReadService service = mock(BazaarReadService.class);
        BazaarController controller = new BazaarController(service);
        List<BazaarQuickFlipDto> expected = List.of(new BazaarQuickFlipDto("A", "Alpha", 100, 90, 10, 10.0, 1000));
        when(service.quickFlips(2.5, 5)).thenReturn(expected);

        List<BazaarQuickFlipDto> response = controller.quickFlips(2.5, 5);

        assertEquals(expected, response);
        verify(service).quickFlips(2.5, 5);
    }
}


