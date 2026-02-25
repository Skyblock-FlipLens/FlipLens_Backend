package com.skyblockflipper.backend.service.item;

import com.skyblockflipper.backend.NEU.NEUClient;
import com.skyblockflipper.backend.NEU.NEUItemMapper;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NeuRepoIngestionServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void ingestLatestFilteredItemsSavesMappedItems() throws Exception {
        NEUClient neuClient = mock(NEUClient.class);
        NEUItemMapper mapper = new NEUItemMapper();
        ItemRepository itemRepository = mock(ItemRepository.class);
        NeuRepoIngestionService service = new NeuRepoIngestionService(neuClient, mapper, itemRepository);

        JsonNode node = objectMapper.readTree("{\"id\":\"ENCHANTED_DIAMOND\",\"displayname\":\"Enchanted Diamond\"}");
        when(neuClient.loadItemJsons()).thenReturn(List.of(node));

        int saved = service.ingestLatestFilteredItems();

        assertEquals(1, saved);
        verify(itemRepository).saveAll(anyList());
    }

    @Test
    void ingestLatestFilteredItemsSkipsSaveWhenMapperReturnsEmpty() throws Exception {
        NEUClient neuClient = mock(NEUClient.class);
        NEUItemMapper mapper = new NEUItemMapper();
        ItemRepository itemRepository = mock(ItemRepository.class);
        NeuRepoIngestionService service = new NeuRepoIngestionService(neuClient, mapper, itemRepository);

        JsonNode nodeWithoutId = objectMapper.readTree("{\"displayname\":\"Unknown\"}");
        when(neuClient.loadItemJsons()).thenReturn(List.of(nodeWithoutId));

        int saved = service.ingestLatestFilteredItems();

        assertEquals(0, saved);
        verify(itemRepository, never()).saveAll(anyList());
    }
}
