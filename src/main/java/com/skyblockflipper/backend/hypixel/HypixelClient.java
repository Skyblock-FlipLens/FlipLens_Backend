package com.skyblockflipper.backend.hypixel;

import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
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
        return fetchAllAuctionPages().getAuctions();
    }

    public AuctionResponse fetchAllAuctionPages() {
        AuctionResponse firstPage = fetchAuctionPage(0);
        if (firstPage == null) {
            throw new IllegalStateException("Failed to fetch auctions page 0 from Hypixel API.");
        }

        List<Auction> allAuctions = new ArrayList<>();
        if (firstPage.getAuctions() != null) {
            allAuctions.addAll(firstPage.getAuctions());
        }
        for (int page = 1; page < firstPage.getTotalPages(); page++) {
            AuctionResponse nextPage = fetchAuctionPage(page);
            if (nextPage == null) {
                throw new IllegalStateException("Failed to fetch auctions page " + page + " from Hypixel API.");
            }
            if (nextPage.getAuctions() != null) {
                allAuctions.addAll(nextPage.getAuctions());
            }
        }
        return new AuctionResponse(
                true,
                0,
                firstPage.getTotalPages(),
                firstPage.getTotalAuctions(),
                firstPage.getLastUpdated(),
                allAuctions
        );
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
        if (result.getAuctions() != null && !result.getAuctions().isEmpty()) {
            log.info(new ObjectMapper().writeValueAsString(result.getAuctions().getFirst().getAuctioneer()));
        }
        log.info(new ObjectMapper().writeValueAsString(result.getLastUpdated()));
    }

    private <T> T request(String uri, ParameterizedTypeReference<T> responseType) {
        try {
            RestClient.RequestHeadersSpec<?> request = restClient.get().uri(uri);
            if (!apiKey.isBlank()) {
                request = request.header("API-Key", apiKey);
            }
            return request.retrieve().body(responseType);
        } catch (RestClientResponseException e) {
            log.warn("Hypixel request failed for {} with status {}: {}", uri, e.getStatusCode(), e.getStatusText());
            return null;
        } catch (RestClientException e) {
            log.warn("Hypixel request failed for {}: {}", uri, e.getMessage());
            return null;
        }
    }
}
