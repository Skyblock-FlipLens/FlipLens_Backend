package com.skyblockflipper.backend.hypixel;

import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class HypixelConditionalClientTest {

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
}
