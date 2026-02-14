package com.skyblockflipper.backend.hypixel;

import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class HypixelClient {
    private final RestClient restClient;
    private final String apiKey;

    public HypixelClient(
            @Value("${config.hypixel.api-url}") String apiUrl,
            @Value("${config.hypixel.api-key:}") String apiKey
    ) {
        this.restClient = RestClient.builder().baseUrl(apiUrl).build();
        this.apiKey = apiKey;
    }

    public AuctionResponse fetchAuctionPage(int page) {
        AuctionResponse result = request(
                "/skyblock/auctions?page=" + page,
                new ParameterizedTypeReference<>() {}
        );
        if (result == null || !result.isSuccess()) {
            return null;
        }
        return result;
    }

    public List<Auction> fetchAllAuctions() {
        AuctionResponse firstPage = fetchAuctionPage(0);
        if (firstPage == null) {
            return List.of();
        }

        List<Auction> allAuctions = new ArrayList<>(firstPage.getAuctions());
        for (int page = 1; page < firstPage.getTotalPages(); page++) {
            AuctionResponse nextPage = fetchAuctionPage(page);
            if (nextPage != null) {
                allAuctions.addAll(nextPage.getAuctions());
            }
        }
        return allAuctions;
    }

    public BazaarResponse fetchBazaar() {
        BazaarResponse result = request(
                "/skyblock/bazaar",
                new ParameterizedTypeReference<>() {}
        );
        if (result == null || !result.isSuccess()) {
            return null;
        }
        return result;
    }

    public void fetchAuctions() {
        AuctionResponse result = fetchAuctionPage(0);
        log.info("-----[FETCH AUCTIONS]-----");
        if (result == null) {
            return;
        }
        log.info(new ObjectMapper().writeValueAsString(result.getTotalAuctions()));
        log.info(new ObjectMapper().writeValueAsString(result.getAuctions().getFirst().getAuctioneer()));
        log.info(new ObjectMapper().writeValueAsString(result.getLastUpdated()));
    }

    private <T> T request(String uri, ParameterizedTypeReference<T> responseType) {
        RestClient.RequestHeadersSpec<?> request = restClient.get().uri(uri);
        if (!apiKey.isBlank()) {
            request = request.header("API-Key", apiKey);
        }
        return request.retrieve().body(responseType);
    }
}
