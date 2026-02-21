package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.service.market.AuctionHouseReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/ah")
@RequiredArgsConstructor
public class AuctionHouseController {

    private final AuctionHouseReadService auctionHouseReadService;

    @GetMapping("/listings/{itemId}")
    public Page<AhListingDto> listings(
            @PathVariable String itemId,
            @RequestParam(required = false) AhListingSortBy sortBy,
            @RequestParam(required = false) Sort.Direction sortDirection,
            @RequestParam(required = false) Boolean bin,
            @RequestParam(required = false) Integer minStars,
            @RequestParam(required = false) Integer maxStars,
            @RequestParam(required = false) String reforge,
            @PageableDefault(size = 20) Pageable pageable
    ) {
        return auctionHouseReadService.listListings(
                itemId,
                sortBy,
                sortDirection,
                bin,
                minStars,
                maxStars,
                reforge,
                pageable
        );
    }

    @GetMapping("/listings/{itemId}/breakdown")
    public AhListingBreakdownDto breakdown(@PathVariable String itemId) {
        return auctionHouseReadService.breakdown(itemId);
    }

    @GetMapping("/recent-sales/{itemId}")
    public List<AhRecentSaleDto> recentSales(
            @PathVariable String itemId,
            @RequestParam(defaultValue = "10") int limit
    ) {
        return auctionHouseReadService.recentSales(itemId, limit);
    }
}
