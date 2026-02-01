package com.skyblockflipper.backend.config.Jobs;

import com.skyblockflipper.backend.NEU.NEUClient;
import com.skyblockflipper.backend.NEU.NEUItemMapper;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.List;

@Component
public class SourceJobs {

    private final NEUClient neuClient;
    private final NEUItemMapper neuItemMapper;
    private final ItemRepository itemRepository;

    @Autowired
    public SourceJobs(NEUClient neuClient, NEUItemMapper neuItemMapper, ItemRepository itemRepository){
        this.neuClient = neuClient;
        this.neuItemMapper = neuItemMapper;
        this.itemRepository = itemRepository;
    }

    @Scheduled(fixedDelayString = "5000")
    public void pollApi() {
        // API call (mit Timeout!), Ergebnis verarbeiten
    }

    @Scheduled(cron = "0 0 2 * * *", zone = "Europe/Vienna")
    public void copyRepoDaily() {
        try {
            List<JsonNode> nodes = neuClient.loadItemJsons();
            for(var x : nodes){
                itemRepository.save(neuItemMapper.fromJson(x));
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
