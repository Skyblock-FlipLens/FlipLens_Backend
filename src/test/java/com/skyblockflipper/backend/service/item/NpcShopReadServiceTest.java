package com.skyblockflipper.backend.service.item;

import com.skyblockflipper.backend.NEU.NEUClient;
import com.skyblockflipper.backend.api.dto.NpcShopOfferDto;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NpcShopReadServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void listsNpcShopOffersAndCalculatesCoinCost() throws Exception {
        NEUClient neuClient = mock(NEUClient.class);
        NpcShopReadService service = new NpcShopReadService(neuClient);

        when(neuClient.loadAllItemJsons()).thenReturn(List.of(
                objectMapper.readTree("""
                        {
                          "internalname":"FARM_MERCHANT_NPC",
                          "displayname":"Farm Merchant",
                          "recipes":[
                            {"type":"npc_shop","cost":["SKYBLOCK_COIN:7"],"result":"WHEAT:3"},
                            {"type":"npc_shop","cost":["MOTE:5"],"result":"RIFT_ITEM:1"}
                          ]
                        }
                        """),
                objectMapper.readTree("""
                        {
                          "internalname":"BLACKSMITH_NPC",
                          "displayname":"Blacksmith",
                          "recipes":[
                            {"type":"npc_shop","cost":["SKYBLOCK_COIN:10"],"result":"ROOKIE_HOE"}
                          ]
                        }
                        """)
        ));

        Page<NpcShopOfferDto> page = service.listNpcBuyableOffers(null, PageRequest.of(0, 10));

        assertEquals(3, page.getTotalElements());
        NpcShopOfferDto wheatOffer = page.getContent().stream()
                .filter(offer -> "WHEAT".equals(offer.itemId()))
                .findFirst()
                .orElseThrow();
        assertEquals(7L, wheatOffer.coinCost());
        assertEquals(7D / 3D, wheatOffer.unitCoinCost());
        assertEquals("FARM_MERCHANT_NPC", wheatOffer.npcId());

        NpcShopOfferDto riftOffer = page.getContent().stream()
                .filter(offer -> "RIFT_ITEM".equals(offer.itemId()))
                .findFirst()
                .orElseThrow();
        assertNull(riftOffer.coinCost());
        assertNull(riftOffer.unitCoinCost());
    }

    @Test
    void filtersByItemIdCaseInsensitive() throws Exception {
        NEUClient neuClient = mock(NEUClient.class);
        NpcShopReadService service = new NpcShopReadService(neuClient);

        when(neuClient.loadAllItemJsons()).thenReturn(List.of(
                objectMapper.readTree("""
                        {
                          "internalname":"FARM_MERCHANT_NPC",
                          "recipes":[
                            {"type":"npc_shop","cost":["SKYBLOCK_COIN:7"],"result":"WHEAT:3"},
                            {"type":"npc_shop","cost":["SKYBLOCK_COIN:10"],"result":"CARROT_ITEM:3"}
                          ]
                        }
                        """)
        ));

        Page<NpcShopOfferDto> page = service.listNpcBuyableOffers("wheat", PageRequest.of(0, 10));

        assertEquals(1, page.getTotalElements());
        assertEquals("WHEAT", page.getContent().getFirst().itemId());
    }
}
