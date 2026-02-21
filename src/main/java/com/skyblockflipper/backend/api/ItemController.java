package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.service.item.ItemReadService;
import com.skyblockflipper.backend.service.item.ItemAnalyticsService;
import com.skyblockflipper.backend.service.item.NpcShopReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/items")
@RequiredArgsConstructor
public class ItemController {

    private final ItemReadService itemReadService;
    private final ItemAnalyticsService itemAnalyticsService;
    private final NpcShopReadService npcShopReadService;

    @GetMapping
    public Page<ItemDto> listItems(
            @RequestParam(required = false) String itemId,
            @RequestParam(required = false) String search,
            @RequestParam(required = false) String category,
            @RequestParam(required = false) String rarity,
            @RequestParam(required = false) MarketplaceType marketplace,
            @PageableDefault(size = 12) Pageable pageable
    ) {
        return itemReadService.listItems(itemId, search, category, rarity, marketplace, pageable);
    }

    @GetMapping("/{itemId}")
    public ResponseEntity<ItemDto> getItem(@PathVariable String itemId) {
        return itemReadService.findItemById(itemId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{itemId}/price-history")
    public List<PricePointDto> priceHistory(
            @PathVariable String itemId,
            @RequestParam(required = false) String range
    ) {
        return itemAnalyticsService.listPriceHistory(itemId, PriceHistoryRange.fromQueryValue(range));
    }

    @GetMapping("/{itemId}/score-history")
    public List<ScorePointDto> scoreHistory(@PathVariable String itemId) {
        return itemAnalyticsService.listScoreHistory(itemId);
    }

    @GetMapping("/{itemId}/quick-stats")
    public ResponseEntity<ItemQuickStatsDto> quickStats(@PathVariable String itemId) {
        return itemAnalyticsService.quickStats(itemId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{itemId}/flips")
    public Page<UnifiedFlipDto> itemFlips(
            @PathVariable String itemId,
            @PageableDefault() Pageable pageable
    ) {
        return itemAnalyticsService.listFlipsForItem(itemId, pageable);
    }

    @GetMapping("/npc-buyable")
    public Page<NpcShopOfferDto> listNpcBuyableItems(
            @RequestParam(required = false) String itemId,
            @PageableDefault(size = 100) Pageable pageable
    ) {
        return npcShopReadService.listNpcBuyableOffers(itemId, pageable);
    }
}
