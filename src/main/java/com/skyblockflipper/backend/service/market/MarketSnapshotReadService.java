package com.skyblockflipper.backend.service.market;

import com.skyblockflipper.backend.api.MarketSnapshotDto;
import com.skyblockflipper.backend.model.market.MarketSnapshotEntity;
import com.skyblockflipper.backend.repository.MarketSnapshotRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class MarketSnapshotReadService {

    private final MarketSnapshotRepository marketSnapshotRepository;

    public MarketSnapshotReadService(MarketSnapshotRepository marketSnapshotRepository) {
        this.marketSnapshotRepository = marketSnapshotRepository;
    }

    @Transactional(readOnly = true)
    public Page<MarketSnapshotDto> listSnapshots(Pageable pageable) {
        return marketSnapshotRepository.findAll(pageable).map(this::toDto);
    }

    private MarketSnapshotDto toDto(MarketSnapshotEntity entity) {
        return new MarketSnapshotDto(
                entity.getId(),
                Instant.ofEpochMilli(entity.getSnapshotTimestampEpochMillis()),
                entity.getAuctionCount(),
                entity.getBazaarProductCount(),
                Instant.ofEpochMilli(entity.getCreatedAtEpochMillis())
        );
    }
}
