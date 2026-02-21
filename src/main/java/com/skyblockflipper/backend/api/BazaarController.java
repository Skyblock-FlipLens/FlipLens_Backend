package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.service.market.BazaarReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/bazaar")
@RequiredArgsConstructor
public class BazaarController {

    private final BazaarReadService bazaarReadService;

    @GetMapping("/{itemId}")
    public ResponseEntity<BazaarProductDto> product(@PathVariable String itemId) {
        return bazaarReadService.getProduct(itemId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{itemId}/orders")
    public BazaarOrderBookDto orderBook(
            @PathVariable String itemId,
            @RequestParam(defaultValue = "15") int depth
    ) {
        return bazaarReadService.getOrderBook(itemId, depth);
    }

    @GetMapping("/quick-flips")
    public List<BazaarQuickFlipDto> quickFlips(
            @RequestParam(required = false) Double minSpreadPct,
            @RequestParam(defaultValue = "20") int limit
    ) {
        return bazaarReadService.quickFlips(minSpreadPct, limit);
    }
}
