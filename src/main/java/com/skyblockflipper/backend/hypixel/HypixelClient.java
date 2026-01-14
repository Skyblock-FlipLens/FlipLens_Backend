package com.skyblockflipper.backend.hypixel;

import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Service
public class HypixelClient {
    private final RestClient restClient;

    public HypixelClient(@Value("${config.hypixel.api-url}") String apiUrl) {
        restClient = RestClient.builder().baseUrl(apiUrl).build();
    }

    public void fetchAuctions() {
        AuctionResponse result = restClient.get()
                .uri("/skyblock/auctions?page=0")
                .retrieve()
                .body(new ParameterizedTypeReference<>() {});
        log.info("-----[FETCH AUCTIONS]-----");
        if (result == null || !result.isSuccess()) {
            return;
        }
        log.info(new ObjectMapper().writeValueAsString(result.getTotalAuctions()));
        log.info(new ObjectMapper().writeValueAsString(result.getAuctions().getFirst().getAuctioneer()));
        log.info(new ObjectMapper().writeValueAsString(result.getLastUpdated()));
    }

}
