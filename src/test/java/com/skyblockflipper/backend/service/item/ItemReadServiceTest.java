package com.skyblockflipper.backend.service.item;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.dto.ItemDto;
import com.skyblockflipper.backend.api.dto.MarketplaceType;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ItemReadServiceTest {

    @Test
    void listItemsAppliesServerSideFiltersAndPagination() {
        ItemRepository itemRepository = mock(ItemRepository.class);
        ItemMarketplaceService marketplaceService = mock(ItemMarketplaceService.class);
        ItemReadService service = new ItemReadService(itemRepository, marketplaceService);

        Item hyperion = item("HYPERION", "Hyperion", "hyperion", "LEGENDARY", "WEAPON");
        Item term = item("TERMINATOR", "Terminator", "terminator", "LEGENDARY", "WEAPON");
        Item diamond = item("ENCHANTED_DIAMOND", "Enchanted Diamond", "enchanted_diamond", "UNCOMMON", "MATERIAL");
        List<Item> items = List.of(hyperion, term, diamond);

        when(itemRepository.findAll(any(Sort.class))).thenReturn(items);
        when(marketplaceService.resolveMarketplaces(items)).thenReturn(Map.of(
                "HYPERION", MarketplaceType.AUCTION_HOUSE,
                "TERMINATOR", MarketplaceType.AUCTION_HOUSE,
                "ENCHANTED_DIAMOND", MarketplaceType.BAZAAR
        ));

        Page<ItemDto> result = service.listItems(
                null,
                "hyper",
                "weapon",
                "legendary",
                MarketplaceType.AUCTION_HOUSE,
                PageRequest.of(0, 12)
        );

        assertEquals(1, result.getTotalElements());
        assertEquals("HYPERION", result.getContent().getFirst().id());
        assertEquals(MarketplaceType.AUCTION_HOUSE, result.getContent().getFirst().marketplace());
    }

    @Test
    void listItemsFallsBackToDefaultSortWhenRepositoryThrows() {
        ItemRepository itemRepository = mock(ItemRepository.class);
        ItemMarketplaceService marketplaceService = mock(ItemMarketplaceService.class);
        ItemReadService service = new ItemReadService(itemRepository, marketplaceService);

        Item item = item("AOTD", "Aspect of the Dragons", "aotd", "LEGENDARY", "WEAPON");
        when(itemRepository.findAll(any(Sort.class)))
                .thenThrow(new RuntimeException("custom sort failed"))
                .thenReturn(List.of(item));
        when(marketplaceService.resolveMarketplaces(List.of(item))).thenReturn(Map.of("AOTD", MarketplaceType.AUCTION_HOUSE));

        Page<ItemDto> result = service.listItems(
                null,
                null,
                null,
                null,
                null,
                PageRequest.of(0, 10, Sort.by("displayName").descending())
        );

        assertEquals(1, result.getTotalElements());
        assertEquals("AOTD", result.getContent().getFirst().id());
        verify(itemRepository).findAll(Sort.by("id").ascending());
    }

    @Test
    void findItemByIdReturnsMappedDtoAndHandlesBlankInput() {
        ItemRepository itemRepository = mock(ItemRepository.class);
        ItemMarketplaceService marketplaceService = mock(ItemMarketplaceService.class);
        ItemReadService service = new ItemReadService(itemRepository, marketplaceService);

        Item item = item("HYPERION", "Hyperion", "hyperion", "LEGENDARY", "WEAPON");
        when(itemRepository.findById("HYPERION")).thenReturn(Optional.of(item));
        when(marketplaceService.resolveMarketplaces(List.of(item))).thenReturn(Map.of("HYPERION", MarketplaceType.AUCTION_HOUSE));

        Optional<ItemDto> found = service.findItemById(" hyperion ");
        Optional<ItemDto> blank = service.findItemById(" ");

        assertTrue(found.isPresent());
        assertEquals("HYPERION", found.get().id());
        assertEquals(MarketplaceType.AUCTION_HOUSE, found.get().marketplace());
        assertFalse(blank.isPresent());
    }

    private Item item(String id, String displayName, String minecraftId, String rarity, String category) {
        return Item.builder()
                .id(id)
                .displayName(displayName)
                .minecraftId(minecraftId)
                .rarity(rarity)
                .category(category)
                .build();
    }
}
