package com.skyblockflipper.backend.repository;

import com.skyblockflipper.backend.model.flippingstorage.FlipTrendSegmentEntity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@Transactional
class FlipTrendSegmentRepositoryTest {

    @Autowired
    private FlipTrendSegmentRepository repository;

    @Test
    void findLatestByFlipKeyInReturnsLatestValidToPerKey() {
        repository.save(segment("key-a", 100L, 200L));
        repository.save(segment("key-a", 210L, 250L));
        repository.save(segment("key-a", 260L, 275L));
        repository.save(segment("key-b", 100L, 120L));
        repository.save(segment("key-b", 121L, 300L));
        repository.save(segment("key-c", 1L, 999L));

        List<FlipTrendSegmentEntity> latest = repository.findLatestByFlipKeyIn(List.of("key-a", "key-b"));

        assertEquals(2, latest.size());
        Map<String, Long> validToByKey = latest.stream()
                .collect(Collectors.toMap(FlipTrendSegmentEntity::getFlipKey,
                        FlipTrendSegmentEntity::getValidToSnapshotEpochMillis));
        assertNotNull(validToByKey.get("key-a"));
        assertNotNull(validToByKey.get("key-b"));
        assertEquals(275L, validToByKey.get("key-a"));
        assertEquals(300L, validToByKey.get("key-b"));
    }

    private FlipTrendSegmentEntity segment(String flipKey, long validFrom, long validTo) {
        FlipTrendSegmentEntity entity = new FlipTrendSegmentEntity();
        entity.setFlipKey(flipKey);
        entity.setValidFromSnapshotEpochMillis(validFrom);
        entity.setValidToSnapshotEpochMillis(validTo);
        entity.setPartial(false);
        entity.setSampleCount(1);
        entity.setCreatedAtEpochMillis(validTo);
        entity.setUpdatedAtEpochMillis(validTo);
        return entity;
    }
}
