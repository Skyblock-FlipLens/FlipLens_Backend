package com.skyblockflipper.backend.config.Jobs;

import com.skyblockflipper.backend.NEU.NEUClient;
import com.skyblockflipper.backend.NEU.NEUItemMapper;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.service.market.MarketDataProcessingService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.List;

@Component
@Slf4j
public class SourceJobs {

    private final NEUClient neuClient;
    private final NEUItemMapper neuItemMapper;
    private final ItemRepository itemRepository;
    private final MarketDataProcessingService marketDataProcessingService;

    @Autowired
    public SourceJobs(NEUClient neuClient,
                      NEUItemMapper neuItemMapper,
                      ItemRepository itemRepository,
                      MarketDataProcessingService marketDataProcessingService){
        this.neuClient = neuClient;
        this.neuItemMapper = neuItemMapper;
        this.itemRepository = itemRepository;
        this.marketDataProcessingService = marketDataProcessingService;
    }

    @Scheduled(fixedDelayString = "5000")
    public void pollApi() {
        try {
            marketDataProcessingService.captureCurrentSnapshotAndPrepareInput();
        } catch (Exception e) {
            log.warn("Failed to poll and persist market snapshot: {}", ExceptionUtils.getStackTrace(e));
        }
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
