package com.skyblockflipper.backend.hypixel;

import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class HypixelConditionalClientTest {

    @Test
    void fetchBazaarSuccessIncludesConditionalHeadersAndBody() throws Exception {
        HypixelConditionalClient client = new HypixelConditionalClient(
                "https://api.hypixel.net/v2",
                "secret",
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)
        );
        RestClient restClient = mock(RestClient.class);
        RestClient.RequestHeadersUriSpec<?> uriSpec =
                (RestClient.RequestHeadersUriSpec<?>) mock(RestClient.RequestHeadersUriSpec.class);
        RestClient.RequestHeadersSpec<?> headersSpec =
                (RestClient.RequestHeadersSpec<?>) mock(RestClient.RequestHeadersSpec.class);
        RestClient.ResponseSpec responseSpec = mock(RestClient.ResponseSpec.class);

        doReturn(uriSpec).when(restClient).get();
        doReturn(headersSpec).when(uriSpec).uri(anyString());
        doReturn(headersSpec).when(headersSpec).header(anyString(), any(String[].class));
        doReturn(responseSpec).when(headersSpec).retrieve();

        BazaarResponse body = new BazaarResponse(true, 123L, null);
        ResponseEntity<BazaarResponse> entity = ResponseEntity.status(200).header(HttpHeaders.ETAG, "\"v1\"").body(body);
        when(responseSpec.toEntity(any(org.springframework.core.ParameterizedTypeReference.class))).thenReturn(entity);
        setField(client, "restClient", restClient);

        HypixelHttpResult<BazaarResponse> result = client.fetchBazaar("/bazaar", "\"etag\"", "Wed, 21 Oct 2015 07:28:00 GMT");

        assertTrue(result.isSuccessful());
        assertEquals(200, result.statusCode());
        assertEquals(body, result.body());
        verify(headersSpec).header("API-Key", "secret");
        verify(headersSpec).header(HttpHeaders.IF_NONE_MATCH, "\"etag\"");
        verify(headersSpec).header(HttpHeaders.IF_MODIFIED_SINCE, "Wed, 21 Oct 2015 07:28:00 GMT");
    }

    @Test
    void fetchBazaarMapsHttpErrorToErrorResult() throws Exception {
        HypixelConditionalClient client = new HypixelConditionalClient(
                "https://api.hypixel.net/v2",
                "",
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)
        );
        RestClient restClient = mock(RestClient.class);
        RestClient.RequestHeadersUriSpec<?> uriSpec =
                (RestClient.RequestHeadersUriSpec<?>) mock(RestClient.RequestHeadersUriSpec.class);
        RestClient.RequestHeadersSpec<?> headersSpec =
                (RestClient.RequestHeadersSpec<?>) mock(RestClient.RequestHeadersSpec.class);
        RestClient.ResponseSpec responseSpec = mock(RestClient.ResponseSpec.class);

        doReturn(uriSpec).when(restClient).get();
        doReturn(headersSpec).when(uriSpec).uri(anyString());
        doReturn(headersSpec).when(headersSpec).header(anyString(), any(String[].class));
        doReturn(responseSpec).when(headersSpec).retrieve();

        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.add(HttpHeaders.RETRY_AFTER, "5");
        HttpServerErrorException exception = HttpServerErrorException.create(
                HttpStatus.BAD_GATEWAY,
                "bad gateway",
                responseHeaders,
                "upstream".getBytes(),
                null
        );
        when(responseSpec.toEntity(any(org.springframework.core.ParameterizedTypeReference.class))).thenThrow(exception);
        setField(client, "restClient", restClient);

        HypixelHttpResult<BazaarResponse> result = client.fetchBazaar("/bazaar", null, null);

        assertFalse(result.isSuccessful());
        assertEquals(502, result.statusCode());
        assertEquals("5", result.headers().getFirst(HttpHeaders.RETRY_AFTER));
    }

    @Test
    void fetchBazaarMapsTransportError() throws Exception {
        HypixelConditionalClient client = new HypixelConditionalClient(
                "https://api.hypixel.net/v2",
                "",
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)
        );
        RestClient restClient = mock(RestClient.class);
        RestClient.RequestHeadersUriSpec<?> uriSpec =
                (RestClient.RequestHeadersUriSpec<?>) mock(RestClient.RequestHeadersUriSpec.class);
        RestClient.RequestHeadersSpec<?> headersSpec =
                (RestClient.RequestHeadersSpec<?>) mock(RestClient.RequestHeadersSpec.class);
        RestClient.ResponseSpec responseSpec = mock(RestClient.ResponseSpec.class);

        doReturn(uriSpec).when(restClient).get();
        doReturn(headersSpec).when(uriSpec).uri(anyString());
        doReturn(headersSpec).when(headersSpec).header(anyString(), any(String[].class));
        doReturn(responseSpec).when(headersSpec).retrieve();

        when(responseSpec.toEntity(any(org.springframework.core.ParameterizedTypeReference.class)))
                .thenThrow(new RestClientException("socket closed"));
        setField(client, "restClient", restClient);

        HypixelHttpResult<BazaarResponse> result = client.fetchBazaar("/bazaar", null, null);

        assertFalse(result.isSuccessful());
        assertTrue(result.transportError());
        assertEquals(0, result.statusCode());
        assertEquals("socket closed", result.errorMessage());
    }

    @Test
    void fetchAllAuctionPagesReturnsErrorForInvalidFirstPage() {
        HypixelConditionalClient client = new HypixelConditionalClient(
                "https://api.hypixel.net/v2",
                "",
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)
        );

        HypixelHttpResult<AuctionResponse> result = client.fetchAllAuctionPages("/skyblock/auctions", null);

        assertEquals(500, result.statusCode());
        assertFalse(result.isSuccessful());
        assertEquals("Invalid first auctions page", result.errorMessage());
    }

    @Test
    void fetchAllAuctionPagesMergesAllPages() {
        HypixelConditionalClient baseClient = new HypixelConditionalClient(
                "https://api.hypixel.net/v2",
                "",
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)
        );
        HypixelConditionalClient client = spy(baseClient);
        AuctionResponse firstPage = new AuctionResponse(
                true,
                0,
                3,
                3,
                123L,
                List.of(auction("a0"))
        );
        doReturn(HypixelHttpResult.success(
                200,
                HttpHeaders.EMPTY,
                new AuctionResponse(true, 1, 3, 3, 123L, List.of(auction("a1")))
        )).when(client).fetchAuctionPage("/skyblock/auctions", 1, null, null);
        doReturn(HypixelHttpResult.success(
                200,
                HttpHeaders.EMPTY,
                new AuctionResponse(true, 2, 3, 3, 123L, List.of(auction("a2")))
        )).when(client).fetchAuctionPage("/skyblock/auctions", 2, null, null);

        HypixelHttpResult<AuctionResponse> result = client.fetchAllAuctionPages("/skyblock/auctions", firstPage);

        assertTrue(result.isSuccessful());
        assertNotNull(result.body());
        assertEquals(3, result.body().getAuctions().size());
        assertEquals("a0", result.body().getAuctions().get(0).getUuid());
        assertEquals("a1", result.body().getAuctions().get(1).getUuid());
        assertEquals("a2", result.body().getAuctions().get(2).getUuid());
        verify(client, times(1)).fetchAuctionPage("/skyblock/auctions", 1, null, null);
        verify(client, times(1)).fetchAuctionPage("/skyblock/auctions", 2, null, null);
    }

    @Test
    void fetchAllAuctionPagesReturnsErrorWhenFollowupFails() {
        HypixelConditionalClient baseClient = new HypixelConditionalClient(
                "https://api.hypixel.net/v2",
                "",
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)
        );
        HypixelConditionalClient client = spy(baseClient);
        AuctionResponse firstPage = new AuctionResponse(
                true,
                0,
                2,
                2,
                123L,
                List.of(auction("a0"))
        );
        doReturn(HypixelHttpResult.error(503, HttpHeaders.EMPTY, "upstream error"))
                .when(client)
                .fetchAuctionPage("/skyblock/auctions", 1, null, null);

        HypixelHttpResult<AuctionResponse> result = client.fetchAllAuctionPages("/skyblock/auctions", firstPage);

        assertFalse(result.isSuccessful());
        assertEquals(503, result.statusCode());
        assertEquals("Failed to fetch auctions page 1", result.errorMessage());
    }

    @Test
    void fetchAllAuctionPagesStreamsViaConsumer() {
        HypixelConditionalClient baseClient = new HypixelConditionalClient(
                "https://api.hypixel.net/v2",
                "",
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)
        );
        HypixelConditionalClient client = spy(baseClient);
        AuctionResponse firstPage = new AuctionResponse(
                true,
                0,
                2,
                2,
                123L,
                List.of(auction("a0"))
        );
        doReturn(HypixelHttpResult.success(
                200,
                HttpHeaders.EMPTY,
                new AuctionResponse(true, 1, 2, 2, 123L, List.of(auction("a1")))
        )).when(client).fetchAuctionPage("/skyblock/auctions", 1, null, null);

        AtomicInteger streamed = new AtomicInteger();
        HypixelHttpResult<HypixelConditionalClient.AuctionScanSummary> result = client.fetchAllAuctionPages(
                "/skyblock/auctions",
                firstPage,
                auction -> streamed.incrementAndGet()
        );

        assertTrue(result.isSuccessful());
        assertNotNull(result.body());
        assertEquals(2, streamed.get());
        assertEquals(2, result.body().totalPages());
        assertEquals(2, result.body().totalAuctions());
        assertEquals(2, result.body().pagesFetched());
        assertEquals(2L, result.body().auctionsSeen());
    }

    @Test
    void sanitizeUsesFallbackForNullZeroAndNegativeDurations() throws Exception {
        HypixelConditionalClient client = new HypixelConditionalClient(
                "https://api.hypixel.net/v2",
                "",
                Duration.ofSeconds(1),
                Duration.ofSeconds(1)
        );
        Method method = HypixelConditionalClient.class.getDeclaredMethod("sanitize", Duration.class, Duration.class);
        method.setAccessible(true);
        Duration fallback = Duration.ofSeconds(8);

        Duration fromNull = (Duration) method.invoke(client, null, fallback);
        Duration fromZero = (Duration) method.invoke(client, Duration.ZERO, fallback);
        Duration fromNegative = (Duration) method.invoke(client, Duration.ofMillis(-1), fallback);
        Duration fromPositive = (Duration) method.invoke(client, Duration.ofSeconds(2), fallback);

        assertEquals(fallback, fromNull);
        assertEquals(fallback, fromZero);
        assertEquals(fallback, fromNegative);
        assertEquals(Duration.ofSeconds(2), fromPositive);
    }

    private Auction auction(String uuid) {
        Auction auction = new Auction();
        auction.setUuid(uuid);
        return auction;
    }

    private void setField(HypixelConditionalClient client, String fieldName, Object value) throws Exception {
        Field field = HypixelConditionalClient.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(client, value);
    }
}
