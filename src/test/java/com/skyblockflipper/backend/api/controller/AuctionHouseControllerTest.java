package com.skyblockflipper.backend.api.controller;

import com.skyblockflipper.backend.api.dto.AhListingBreakdownDto;
import com.skyblockflipper.backend.api.dto.AhListingDto;
import com.skyblockflipper.backend.api.dto.AhListingSortBy;
import com.skyblockflipper.backend.api.dto.AhRecentSaleDto;
import com.skyblockflipper.backend.service.market.AuctionHouseReadService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AuctionHouseControllerTest {

    @Test
    void listingsBuildsPageableAndDelegatesToReadService() {
        AuctionHouseReadService readService = mock(AuctionHouseReadService.class);
        AuctionHouseController controller = new AuctionHouseController(readService);
        Page<AhListingDto> expected = new PageImpl<>(List.of(
                new AhListingDto("auction-1", "HYPERION", "Hyperion", 1_000_000L, List.of("smite"), "LEGENDARY", 5,
                        "Withered", Instant.parse("2026-03-01T10:00:00Z"), true, 1_200_000L, 10, List.of("sapphire"))
        ));
        when(readService.listListings(eq("hyperion"), eq(AhListingSortBy.PRICE), eq(Sort.Direction.DESC), eq(true),
                eq(3), eq(5), eq("Withered"), any(Pageable.class))).thenReturn(expected);

        Page<AhListingDto> actual = controller.listings(
                "hyperion",
                AhListingSortBy.PRICE,
                Sort.Direction.DESC,
                true,
                3,
                5,
                "Withered",
                2,
                50
        );

        assertSame(expected, actual);

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(readService).listListings(eq("hyperion"), eq(AhListingSortBy.PRICE), eq(Sort.Direction.DESC), eq(true),
                eq(3), eq(5), eq("Withered"), pageableCaptor.capture());
        Pageable pageable = pageableCaptor.getValue();
        assertEquals(2, pageable.getPageNumber());
        assertEquals(50, pageable.getPageSize());
        Sort sort = pageable.getSort();
        assertNotNull(sort);
        Sort.Order order = sort.getOrderFor("id");
        assertNotNull(order);
        assertEquals(Sort.Direction.ASC, order.getDirection());
    }

    @Test
    void listingsUsesDefaultPaginationBoundsWhenInputsAreMissingOrOversized() {
        AuctionHouseReadService readService = mock(AuctionHouseReadService.class);
        AuctionHouseController controller = new AuctionHouseController(readService);
        when(readService.listListings(eq("terminator"), eq(null), eq(null), eq(null),
                eq(null), eq(null), eq(null), any(Pageable.class))).thenReturn(Page.empty());

        controller.listings("terminator", null, null, null, null, null, null, -5, 10_000);

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(readService).listListings(eq("terminator"), eq(null), eq(null), eq(null),
                eq(null), eq(null), eq(null), pageableCaptor.capture());
        Pageable pageable = pageableCaptor.getValue();
        assertEquals(0, pageable.getPageNumber());
        assertEquals(1_000, pageable.getPageSize());
        Sort sort = pageable.getSort();
        assertNotNull(sort);
        Sort.Order order = sort.getOrderFor("id");
        assertNotNull(order);
        assertEquals(Sort.Direction.ASC, order.getDirection());
    }

    @Test
    void breakdownAndRecentSalesDelegateToReadService() {
        AuctionHouseReadService readService = mock(AuctionHouseReadService.class);
        AuctionHouseController controller = new AuctionHouseController(readService);
        AhListingBreakdownDto breakdown = new AhListingBreakdownDto(4L, Map.of("5", 2L), Map.of("BIN", 4L), Map.of("withered", 1L), 99_000L, 90_000L);
        List<AhRecentSaleDto> recentSales = List.of(new AhRecentSaleDto("sale-1", 123_456L, 5, "Precise", Instant.parse("2026-03-01T09:00:00Z"), true));
        when(readService.breakdown("juju_shortbow")).thenReturn(breakdown);
        when(readService.recentSales("juju_shortbow", 7)).thenReturn(recentSales);

        assertSame(breakdown, controller.breakdown("juju_shortbow"));
        assertSame(recentSales, controller.recentSales("juju_shortbow", 7));
    }
}
