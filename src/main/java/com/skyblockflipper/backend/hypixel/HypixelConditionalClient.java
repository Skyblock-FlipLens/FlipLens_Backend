package com.skyblockflipper.backend.hypixel;

import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HypixelConditionalClient {

    private final RestClient restClient;
    private final String apiKey;

    public HypixelConditionalClient(String apiUrl, String apiKey, Duration connectTimeout, Duration requestTimeout) {
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(sanitize(connectTimeout, Duration.ofSeconds(2)))
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(sanitize(requestTimeout, Duration.ofSeconds(8)));
        this.restClient = RestClient.builder()
                .baseUrl(apiUrl)
                .requestFactory(requestFactory)
                .build();
        this.apiKey = apiKey == null ? "" : apiKey;
    }

    public HypixelHttpResult<AuctionResponse> fetchAuctionPage(String path, int page, String ifNoneMatch, String ifModifiedSince) {
        return request(path + "?page=" + page, ifNoneMatch, ifModifiedSince, new ParameterizedTypeReference<>() {});
    }

    public HypixelHttpResult<BazaarResponse> fetchBazaar(String path, String ifNoneMatch, String ifModifiedSince) {
        return request(path, ifNoneMatch, ifModifiedSince, new ParameterizedTypeReference<>() {});
    }

    public HypixelHttpResult<AuctionResponse> fetchAllAuctionPages(String auctionsPath, AuctionResponse firstPage) {
        if (firstPage == null || !firstPage.isSuccess()) {
            return HypixelHttpResult.error(500, HttpHeaders.EMPTY, "Invalid first auctions page");
        }
        List<Auction> allAuctions = new ArrayList<>();
        if (firstPage.getAuctions() != null) {
            allAuctions.addAll(firstPage.getAuctions());
        }

        for (int page = 1; page < firstPage.getTotalPages(); page++) {
            HypixelHttpResult<AuctionResponse> nextPageResult = fetchAuctionPage(auctionsPath, page, null, null);
            if (!nextPageResult.isSuccessful() || nextPageResult.body() == null || !nextPageResult.body().isSuccess()) {
                return HypixelHttpResult.error(
                        nextPageResult.statusCode() == 0 ? 500 : nextPageResult.statusCode(),
                        nextPageResult.headers(),
                        "Failed to fetch auctions page " + page
                );
            }
            if (nextPageResult.body().getAuctions() != null) {
                allAuctions.addAll(nextPageResult.body().getAuctions());
            }
        }

        AuctionResponse merged = new AuctionResponse(
                true,
                0,
                firstPage.getTotalPages(),
                firstPage.getTotalAuctions(),
                firstPage.getLastUpdated(),
                allAuctions
        );
        return HypixelHttpResult.success(200, HttpHeaders.EMPTY, merged);
    }

    private <T> HypixelHttpResult<T> request(String uri,
                                             String ifNoneMatch,
                                             String ifModifiedSince,
                                             ParameterizedTypeReference<T> responseType) {
        try {
            RestClient.RequestHeadersSpec<?> request = restClient.get().uri(uri);
            if (StringUtils.hasText(apiKey)) {
                request = request.header("API-Key", apiKey);
            }
            if (StringUtils.hasText(ifNoneMatch)) {
                request = request.header(HttpHeaders.IF_NONE_MATCH, ifNoneMatch);
            }
            if (StringUtils.hasText(ifModifiedSince)) {
                request = request.header(HttpHeaders.IF_MODIFIED_SINCE, ifModifiedSince);
            }
            ResponseEntity<T> entity = request.retrieve().toEntity(responseType);
            return HypixelHttpResult.success(entity.getStatusCode().value(), entity.getHeaders(), entity.getBody());
        } catch (RestClientResponseException e) {
            HttpHeaders headers = e.getResponseHeaders() == null ? HttpHeaders.EMPTY : e.getResponseHeaders();
            log.debug("Hypixel conditional request failed for {} status {}: {}", uri, e.getStatusCode(), e.getMessage());
            return HypixelHttpResult.error(e.getStatusCode().value(), headers, e.getMessage());
        } catch (RestClientException e) {
            log.debug("Hypixel conditional request transport error for {}: {}", uri, e.getMessage());
            return HypixelHttpResult.transportError(e.getMessage());
        }
    }

    private Duration sanitize(Duration configured, Duration fallback) {
        if (configured == null || configured.isNegative() || configured.isZero()) {
            return fallback;
        }
        return configured;
    }
}
