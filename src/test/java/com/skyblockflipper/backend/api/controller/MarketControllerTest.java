package com.skyblockflipper.backend.api.controller;

import com.skyblockflipper.backend.api.dto.*;

import com.skyblockflipper.backend.service.market.MarketOverviewService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(MarketController.class)
class MarketControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private MarketOverviewService marketOverviewService;

    @Test
    void overviewReturnsMarketSummary() throws Exception {
        when(marketOverviewService.overview("HYPERION"))
                .thenReturn(new MarketOverviewDto(
                        "HYPERION",
                        Instant.parse("2026-02-20T12:00:00Z"),
                        789_400_000D,
                        -0.7D,
                        770_700_000D,
                        -1.6D,
                        18_700_000D,
                        2.4D,
                        857_500_000D,
                        770_700_000D,
                        416L,
                        292D,
                        2L,
                        22_500_000L
                ));

        mockMvc.perform(get("/api/v1/market/overview").param("productId", "HYPERION"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.productId").value("HYPERION"))
                .andExpect(jsonPath("$.buy").value(789400000))
                .andExpect(jsonPath("$.sell").value(770700000))
                .andExpect(jsonPath("$.spread").value(18700000))
                .andExpect(jsonPath("$.activeFlips").value(2));

        verify(marketOverviewService, times(1)).overview("HYPERION");
    }
}


