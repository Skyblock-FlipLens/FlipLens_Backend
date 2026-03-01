package com.skyblockflipper.backend.api.controller;

import com.skyblockflipper.backend.api.dto.*;

import com.skyblockflipper.backend.service.market.MarketOverviewService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/market")
@RequiredArgsConstructor
public class MarketController {

    private final MarketOverviewService marketOverviewService;

    @GetMapping("/overview")
    public MarketOverviewDto marketOverview(@RequestParam(required = false) String productId) {
        return marketOverviewService.overview(productId);
    }
}



