package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.repository.MarketSnapshotRepository;
import com.skyblockflipper.backend.repository.RetainedMarketSnapshotRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class MarketSnapshotReadServiceTest {

    @Autowired
    private MarketSnapshotPersistenceService marketSnapshotPersistenceService;

    @Autowired
    private MarketSnapshotReadService marketSnapshotReadService;

    @Autowired
    private MarketSnapshotRepository marketSnapshotRepository;

    @Autowired
    private RetainedMarketSnapshotRepository retainedMarketSnapshotRepository;

    @BeforeEach
    void clean() {
        retainedMarketSnapshotRepository.deleteAll();
        marketSnapshotRepository.deleteAll();
    }

    @AfterEach
    void cleanAfterEach() {
        clean();
    }

    @Test
    void listSnapshotsIncludesRetainedAndRawHistoryInDescendingOrder() {
        saveAt("2026-02-17T11:59:20Z");
        saveAt("2026-02-17T11:57:10Z");
        saveAt("2026-02-17T11:57:05Z");

        marketSnapshotPersistenceService.compactSnapshots(Instant.parse("2026-02-17T12:00:00Z"));

        Page<com.skyblockflipper.backend.api.dto.MarketSnapshotDto> page =
                marketSnapshotReadService.listSnapshots(PageRequest.of(0, 10));

        assertEquals(2, page.getTotalElements());
        assertEquals(List.of(
                        Instant.parse("2026-02-17T11:59:20Z"),
                        Instant.parse("2026-02-17T11:57:05Z")
                ),
                page.getContent().stream().map(com.skyblockflipper.backend.api.dto.MarketSnapshotDto::snapshotTimestamp).toList());
    }

    private void saveAt(String timestamp) {
        marketSnapshotPersistenceService.save(new MarketSnapshot(
                Instant.parse(timestamp),
                List.of(),
                Map.of()
        ));
    }
}
