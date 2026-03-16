package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.api.dto.MarketSnapshotDto;
import com.skyblockflipper.backend.repository.MarketSnapshotHistoryRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class MarketSnapshotReadService {

    private final MarketSnapshotHistoryRepository marketSnapshotHistoryRepository;

    public MarketSnapshotReadService(MarketSnapshotHistoryRepository marketSnapshotHistoryRepository) {
        this.marketSnapshotHistoryRepository = marketSnapshotHistoryRepository;
    }

    @Transactional(readOnly = true)
    public Page<MarketSnapshotDto> listSnapshots(Pageable pageable) {
        return marketSnapshotHistoryRepository.findCombinedSnapshotSummaries(pageable).map(this::toDto);
    }

    private MarketSnapshotDto toDto(MarketSnapshotHistoryRepository.MarketSnapshotSummaryProjection entity) {
        return new MarketSnapshotDto(
                entity.getIdText() == null ? null : UUID.fromString(entity.getIdText()),
                Instant.ofEpochMilli(entity.getSnapshotTimestampEpochMillis()),
                entity.getAuctionCount(),
                entity.getBazaarProductCount(),
                Instant.ofEpochMilli(entity.getCreatedAtEpochMillis())
        );
    }
}
