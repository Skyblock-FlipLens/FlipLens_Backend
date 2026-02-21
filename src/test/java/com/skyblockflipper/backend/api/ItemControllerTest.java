package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.service.item.ItemReadService;
import com.skyblockflipper.backend.service.item.ItemAnalyticsService;
import com.skyblockflipper.backend.service.item.NpcShopReadService;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ItemControllerTest {

    @Test
    void listItemsDelegatesToService() {
        ItemReadService itemReadService = mock(ItemReadService.class);
        ItemAnalyticsService itemAnalyticsService = mock(ItemAnalyticsService.class);
        NpcShopReadService npcShopReadService = mock(NpcShopReadService.class);
        ItemController controller = new ItemController(itemReadService, itemAnalyticsService, npcShopReadService);
        Pageable pageable = PageRequest.of(0, 100);
        ItemDto dto = new ItemDto("WHEAT", "Wheat", "minecraft:wheat", "COMMON", "FARMING", List.of());
        Page<ItemDto> expected = new PageImpl<>(List.of(dto), pageable, 1);

        when(itemReadService.listItems("WHEAT", null, null, null, null, pageable)).thenReturn(expected);

        Page<ItemDto> response = controller.listItems("WHEAT", null, null, null, null, pageable);

        assertEquals(expected, response);
        verify(itemReadService).listItems("WHEAT", null, null, null, null, pageable);
    }

    @Test
    void listNpcBuyableItemsDelegatesToService() {
        ItemReadService itemReadService = mock(ItemReadService.class);
        ItemAnalyticsService itemAnalyticsService = mock(ItemAnalyticsService.class);
        NpcShopReadService service = mock(NpcShopReadService.class);
        ItemController controller = new ItemController(itemReadService, itemAnalyticsService, service);
        Pageable pageable = PageRequest.of(0, 100);
        NpcShopOfferDto dto = new NpcShopOfferDto(
                "FARM_MERCHANT_NPC",
                "Farm Merchant",
                "WHEAT",
                3,
                List.of(new NpcShopOfferDto.CostDto("SKYBLOCK_COIN", 7)),
                7L,
                7D / 3D
        );
        Page<NpcShopOfferDto> expected = new PageImpl<>(List.of(dto), pageable, 1);

        when(service.listNpcBuyableOffers("WHEAT", pageable)).thenReturn(expected);

        Page<NpcShopOfferDto> response = controller.listNpcBuyableItems("WHEAT", pageable);

        assertEquals(expected, response);
        verify(service).listNpcBuyableOffers("WHEAT", pageable);
    }

    @Test
    void getItemReturnsOkWhenFoundAndNotFoundWhenMissing() {
        ItemReadService itemReadService = mock(ItemReadService.class);
        ItemAnalyticsService itemAnalyticsService = mock(ItemAnalyticsService.class);
        NpcShopReadService npcShopReadService = mock(NpcShopReadService.class);
        ItemController controller = new ItemController(itemReadService, itemAnalyticsService, npcShopReadService);
        ItemDto dto = new ItemDto("HYPERION", "Hyperion", "hyperion", "LEGENDARY", "WEAPON", List.of());

        when(itemReadService.findItemById("HYPERION")).thenReturn(Optional.of(dto));
        when(itemReadService.findItemById("MISSING")).thenReturn(Optional.empty());

        ResponseEntity<ItemDto> found = controller.getItem("HYPERION");
        ResponseEntity<ItemDto> missing = controller.getItem("MISSING");

        assertEquals(HttpStatus.OK, found.getStatusCode());
        assertEquals(dto, found.getBody());
        assertEquals(HttpStatus.NOT_FOUND, missing.getStatusCode());
    }

    @Test
    void priceHistoryDelegatesToAnalyticsService() {
        ItemReadService itemReadService = mock(ItemReadService.class);
        ItemAnalyticsService itemAnalyticsService = mock(ItemAnalyticsService.class);
        NpcShopReadService npcShopReadService = mock(NpcShopReadService.class);
        ItemController controller = new ItemController(itemReadService, itemAnalyticsService, npcShopReadService);

        List<PricePointDto> pricePoints = List.of(new PricePointDto(Instant.parse("2026-02-21T00:00:00Z"), 100L, 90L, 10L));
        when(itemAnalyticsService.listPriceHistory("HYPERION", PriceHistoryRange.D7)).thenReturn(pricePoints);

        assertEquals(pricePoints, controller.priceHistory("HYPERION", "7d"));
        verify(itemAnalyticsService).listPriceHistory("HYPERION", PriceHistoryRange.D7);
    }

    @Test
    void scoreHistoryDelegatesToAnalyticsService() {
        ItemReadService itemReadService = mock(ItemReadService.class);
        ItemAnalyticsService itemAnalyticsService = mock(ItemAnalyticsService.class);
        NpcShopReadService npcShopReadService = mock(NpcShopReadService.class);
        ItemController controller = new ItemController(itemReadService, itemAnalyticsService, npcShopReadService);

        List<ScorePointDto> scorePoints = List.of(new ScorePointDto(Instant.parse("2026-02-21T00:00:00Z"), 70D, 20D));
        when(itemAnalyticsService.listScoreHistory("HYPERION")).thenReturn(scorePoints);

        assertEquals(scorePoints, controller.scoreHistory("HYPERION"));
        verify(itemAnalyticsService).listScoreHistory("HYPERION");
    }

    @Test
    void quickStatsDelegatesToAnalyticsService() {
        ItemReadService itemReadService = mock(ItemReadService.class);
        ItemAnalyticsService itemAnalyticsService = mock(ItemAnalyticsService.class);
        NpcShopReadService npcShopReadService = mock(NpcShopReadService.class);
        ItemController controller = new ItemController(itemReadService, itemAnalyticsService, npcShopReadService);

        ItemQuickStatsDto quickStatsDto = new ItemQuickStatsDto(100L, 90L, 1D, -1D, 10L, 10D, 10L, 9L, 110L, 80L);
        when(itemAnalyticsService.quickStats("HYPERION")).thenReturn(Optional.of(quickStatsDto));

        assertEquals(HttpStatus.OK, controller.quickStats("HYPERION").getStatusCode());
        verify(itemAnalyticsService).quickStats("HYPERION");
    }

    @Test
    void quickStatsNotFoundDelegatesToAnalyticsService() {
        ItemReadService itemReadService = mock(ItemReadService.class);
        ItemAnalyticsService itemAnalyticsService = mock(ItemAnalyticsService.class);
        NpcShopReadService npcShopReadService = mock(NpcShopReadService.class);
        ItemController controller = new ItemController(itemReadService, itemAnalyticsService, npcShopReadService);

        when(itemAnalyticsService.quickStats("MISSING")).thenReturn(Optional.empty());

        assertEquals(HttpStatus.NOT_FOUND, controller.quickStats("MISSING").getStatusCode());
        verify(itemAnalyticsService).quickStats("MISSING");
    }

    @Test
    void itemFlipsDelegatesToAnalyticsService() {
        ItemReadService itemReadService = mock(ItemReadService.class);
        ItemAnalyticsService itemAnalyticsService = mock(ItemAnalyticsService.class);
        NpcShopReadService npcShopReadService = mock(NpcShopReadService.class);
        ItemController controller = new ItemController(itemReadService, itemAnalyticsService, npcShopReadService);

        Pageable pageable = PageRequest.of(0, 10);
        Page<UnifiedFlipDto> flips = new PageImpl<>(List.of(
                new UnifiedFlipDto(null, null, List.of(), List.of(), null, null, null, null, null, null, 0.65D, 0.35D, Instant.parse("2026-02-21T00:00:00Z"), false, List.of(), List.of(), List.of())
        ), PageRequest.of(0, 10), 1);
        when(itemAnalyticsService.listFlipsForItem("HYPERION", pageable)).thenReturn(flips);

        assertEquals(flips, controller.itemFlips("HYPERION", pageable));
        verify(itemAnalyticsService).listFlipsForItem("HYPERION", pageable);
    }
}
