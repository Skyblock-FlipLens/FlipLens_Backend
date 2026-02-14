package com.skyblockflipper.backend.hypixel;

import com.skyblockflipper.backend.hypixel.model.Auction;
import com.skyblockflipper.backend.hypixel.model.AuctionResponse;
import com.skyblockflipper.backend.hypixel.model.BazaarProduct;
import com.skyblockflipper.backend.hypixel.model.BazaarQuickStatus;
import com.skyblockflipper.backend.hypixel.model.BazaarResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HypixelClientTest {

    @Test
    void fetchAuctionsHandlesNullResponse() {
        RestClient restClient = mock(RestClient.class, Answers.RETURNS_DEEP_STUBS);
        when(restClient.get().uri(anyString()).retrieve().body(any(ParameterizedTypeReference.class)))
                .thenReturn(null);

        HypixelClient client = new HypixelClient("http://localhost", "");
        ReflectionTestUtils.setField(client, "restClient", restClient);

        client.fetchAuctions();

        verify(restClient, atLeastOnce()).get();
    }

    @Test
    void fetchAuctionsHandlesSuccess() {
        RestClient restClient = mock(RestClient.class, Answers.RETURNS_DEEP_STUBS);
        Auction auction = new Auction(
                "uuid",
                "auctioneer",
                "profile",
                List.of(),
                1L,
                2L,
                "item",
                "lore",
                "extra",
                "category",
                "tier",
                100L,
                false,
                List.of(),
                150L,
                List.of()
        );
        AuctionResponse response = new AuctionResponse(true, 0, 1, 1, 3L, List.of(auction));
        when(restClient.get().uri(anyString()).retrieve().body(any(ParameterizedTypeReference.class)))
                .thenReturn(response);

        HypixelClient client = new HypixelClient("http://localhost", "");
        ReflectionTestUtils.setField(client, "restClient", restClient);

        client.fetchAuctions();

        verify(restClient, atLeastOnce()).get();
    }

    @Test
    void fetchAllAuctionsLoadsAllPages() {
        RestClient restClient = mock(RestClient.class, Answers.RETURNS_DEEP_STUBS);

        Auction auction1 = new Auction("uuid1", "a1", "p1", List.of(), 1L, 2L, "item1", "lore1", "e1", "c1", "COMMON", 100L, true, List.of(), 100L, List.of());
        Auction auction2 = new Auction("uuid2", "a2", "p2", List.of(), 1L, 2L, "item2", "lore2", "e2", "c2", "RARE", 200L, true, List.of(), 200L, List.of());
        AuctionResponse page0 = new AuctionResponse(true, 0, 2, 2, 3L, List.of(auction1));
        AuctionResponse page1 = new AuctionResponse(true, 1, 2, 2, 4L, List.of(auction2));

        when(restClient.get().uri(anyString()).retrieve().body(any(ParameterizedTypeReference.class)))
                .thenReturn(page0, page1);

        HypixelClient client = new HypixelClient("http://localhost", "");
        ReflectionTestUtils.setField(client, "restClient", restClient);

        List<Auction> auctions = client.fetchAllAuctions();

        assertEquals(2, auctions.size());
        verify(restClient.get(), atLeastOnce()).uri("/skyblock/auctions?page=0");
        verify(restClient.get(), atLeastOnce()).uri("/skyblock/auctions?page=1");
    }

    @Test
    void fetchBazaarReturnsProducts() {
        RestClient restClient = mock(RestClient.class, Answers.RETURNS_DEEP_STUBS);

        BazaarQuickStatus quickStatus = new BazaarQuickStatus(10.0, 9.5, 1000, 900, 10000, 9000);
        BazaarProduct product = new BazaarProduct("ENCHANTED_DIAMOND", quickStatus);
        BazaarResponse response = new BazaarResponse(true, 5L, Map.of("ENCHANTED_DIAMOND", product));

        when(restClient.get().uri(anyString()).retrieve().body(any(ParameterizedTypeReference.class)))
                .thenReturn(response);

        HypixelClient client = new HypixelClient("http://localhost", "");
        ReflectionTestUtils.setField(client, "restClient", restClient);

        BazaarResponse bazaar = client.fetchBazaar();

        assertNotNull(bazaar);
        assertEquals(1, bazaar.getProducts().size());
        verify(restClient.get(), atLeastOnce()).uri("/skyblock/bazaar");
    }
}
