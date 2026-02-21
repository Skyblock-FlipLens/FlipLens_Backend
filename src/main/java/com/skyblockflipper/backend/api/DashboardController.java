package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.service.market.DashboardReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/dashboard")
@RequiredArgsConstructor
public class DashboardController {

    private final DashboardReadService dashboardReadService;

    @GetMapping("/overview")
    public DashboardOverviewDto overview() {
        return dashboardReadService.overview();
    }

    @GetMapping("/trending")
    public List<TrendingItemDto> trending(@RequestParam(defaultValue = "10") int limit) {
        return dashboardReadService.trending(limit);
    }
}
