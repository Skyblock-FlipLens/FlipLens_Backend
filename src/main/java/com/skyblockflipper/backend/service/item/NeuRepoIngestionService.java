package com.skyblockflipper.backend.service.item;

import com.skyblockflipper.backend.NEU.NEUClient;
import com.skyblockflipper.backend.NEU.NEUItemMapper;
import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.List;

@Service
public class NeuRepoIngestionService {

    private final NEUClient neuClient;
    private final NEUItemMapper neuItemMapper;
    private final ItemRepository itemRepository;

    public NeuRepoIngestionService(NEUClient neuClient,
                                   NEUItemMapper neuItemMapper,
                                   ItemRepository itemRepository) {
        this.neuClient = neuClient;
        this.neuItemMapper = neuItemMapper;
        this.itemRepository = itemRepository;
    }

    public synchronized int ingestLatestFilteredItems() throws IOException, InterruptedException {
        List<JsonNode> nodes = neuClient.loadItemJsons();
        List<Item> items = neuItemMapper.fromJson(nodes);
        if (items.isEmpty()) {
            return 0;
        }
        itemRepository.saveAll(items);
        return items.size();
    }
}
