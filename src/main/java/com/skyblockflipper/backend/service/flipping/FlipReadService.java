package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.FlipCoverageDto;
import com.skyblockflipper.backend.api.FlipSnapshotStatsDto;
import com.skyblockflipper.backend.api.FlipTypesDto;
import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.repository.FlipRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class FlipReadService {

    private static final List<FlipType> COVERED_FLIP_TYPES = List.of(
            FlipType.AUCTION,
            FlipType.BAZAAR,
            FlipType.CRAFTING,
            FlipType.FORGE
    );
    private static final List<String> EXCLUDED_FLIP_TYPES = List.of("SHARD", "FUSION");

    private final FlipRepository flipRepository;
    private final UnifiedFlipDtoMapper unifiedFlipDtoMapper;
    private final FlipCalculationContextService flipCalculationContextService;

    public FlipReadService(FlipRepository flipRepository,
                           UnifiedFlipDtoMapper unifiedFlipDtoMapper,
                           FlipCalculationContextService flipCalculationContextService) {
        this.flipRepository = flipRepository;
        this.unifiedFlipDtoMapper = unifiedFlipDtoMapper;
        this.flipCalculationContextService = flipCalculationContextService;
    }

    public Page<UnifiedFlipDto> listFlips(FlipType flipType, Pageable pageable) {
        return listFlips(flipType, null, pageable);
    }

    public Page<UnifiedFlipDto> listFlips(FlipType flipType, Instant snapshotTimestamp, Pageable pageable) {
        Long snapshotEpochMillis = resolveSnapshotEpochMillis(snapshotTimestamp);
        FlipCalculationContext context = snapshotEpochMillis == null
                ? flipCalculationContextService.loadCurrentContext()
                : flipCalculationContextService.loadContextAsOf(Instant.ofEpochMilli(snapshotEpochMillis));

        Page<Flip> flips = queryFlips(flipType, snapshotEpochMillis, pageable);
        return flips.map(flip -> unifiedFlipDtoMapper.toDto(flip, context));
    }

    public Optional<UnifiedFlipDto> findFlipById(UUID id) {
        Optional<Flip> flip = flipRepository.findById(id);
        if (flip.isEmpty()) {
            return Optional.empty();
        }
        Long snapshotEpochMillis = flip.get().getSnapshotTimestampEpochMillis();
        FlipCalculationContext context = snapshotEpochMillis == null
                ? flipCalculationContextService.loadCurrentContext()
                : flipCalculationContextService.loadContextAsOf(Instant.ofEpochMilli(snapshotEpochMillis));
        return flip.map(value -> unifiedFlipDtoMapper.toDto(value, context));
    }

    public FlipTypesDto listSupportedFlipTypes() {
        List<FlipType> flipTypes = Arrays.stream(FlipType.values())
                .sorted(Comparator.comparing(Enum::name))
                .toList();
        return new FlipTypesDto(flipTypes);
    }

    public FlipSnapshotStatsDto snapshotStats(Instant snapshotTimestamp) {
        Long snapshotEpochMillis = resolveSnapshotEpochMillis(snapshotTimestamp);
        if (snapshotEpochMillis == null) {
            return new FlipSnapshotStatsDto(null, 0L, emptyTypeCounts());
        }

        EnumMap<FlipType, Long> countsByType = new EnumMap<>(FlipType.class);
        for (FlipType flipType : FlipType.values()) {
            countsByType.put(flipType, 0L);
        }

        List<Object[]> rows = flipRepository.countByFlipTypeForSnapshot(snapshotEpochMillis);
        long total = 0L;
        for (Object[] row : rows) {
            if (row == null || row.length < 2) {
                continue;
            }
            if (!(row[0] instanceof FlipType flipType)) {
                continue;
            }
            long count = toLong(row[1]);
            countsByType.put(flipType, count);
            total += count;
        }

        List<FlipSnapshotStatsDto.FlipTypeCountDto> byType = countsByType.entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().name()))
                .map(entry -> new FlipSnapshotStatsDto.FlipTypeCountDto(entry.getKey(), entry.getValue()))
                .toList();
        return new FlipSnapshotStatsDto(Instant.ofEpochMilli(snapshotEpochMillis), total, byType);
    }

    public FlipCoverageDto flipTypeCoverage() {
        Long snapshotEpochMillis = resolveSnapshotEpochMillis(null);
        EnumMap<FlipType, Long> countsByType = new EnumMap<>(FlipType.class);
        for (FlipType flipType : COVERED_FLIP_TYPES) {
            countsByType.put(flipType, 0L);
        }

        if (snapshotEpochMillis != null) {
            List<Object[]> rows = flipRepository.countByFlipTypeForSnapshot(snapshotEpochMillis);
            for (Object[] row : rows) {
                if (row == null || row.length < 2) {
                    continue;
                }
                if (!(row[0] instanceof FlipType flipType) || !countsByType.containsKey(flipType)) {
                    continue;
                }
                countsByType.put(flipType, toLong(row[1]));
            }
        }

        List<FlipCoverageDto.FlipTypeCoverageDto> coverage = COVERED_FLIP_TYPES.stream()
                .map(type -> coverageEntry(type, countsByType.getOrDefault(type, 0L)))
                .toList();

        Instant snapshotTimestamp = snapshotEpochMillis == null ? null : Instant.ofEpochMilli(snapshotEpochMillis);
        return new FlipCoverageDto(snapshotTimestamp, EXCLUDED_FLIP_TYPES, coverage);
    }

    private Long resolveSnapshotEpochMillis(Instant snapshotTimestamp) {
        if (snapshotTimestamp != null) {
            return snapshotTimestamp.toEpochMilli();
        }
        Optional<Long> latestSnapshot = flipRepository.findMaxSnapshotTimestampEpochMillis();
        return latestSnapshot.orElse(null);
    }

    private FlipCoverageDto.FlipTypeCoverageDto coverageEntry(FlipType flipType, long latestSnapshotCount) {
        FlipCoverageDto.CoverageStatus supported = FlipCoverageDto.CoverageStatus.SUPPORTED;
        String notes = switch (flipType) {
            case AUCTION, BAZAAR -> "Generated from Hypixel market snapshots via MarketFlipMapper.";
            case CRAFTING, FORGE -> "Generated from NEU recipes via RecipeToFlipMapper.";
            default -> "Not part of the active coverage matrix.";
        };
        return new FlipCoverageDto.FlipTypeCoverageDto(
                flipType,
                supported,
                supported,
                supported,
                supported,
                latestSnapshotCount,
                notes
        );
    }

    private long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return 0L;
    }

    private List<FlipSnapshotStatsDto.FlipTypeCountDto> emptyTypeCounts() {
        List<FlipSnapshotStatsDto.FlipTypeCountDto> byType = new ArrayList<>();
        for (FlipType flipType : FlipType.values()) {
            byType.add(new FlipSnapshotStatsDto.FlipTypeCountDto(flipType, 0L));
        }
        byType.sort(Comparator.comparing(item -> item.flipType().name()));
        return byType;
    }

    private Page<Flip> queryFlips(FlipType flipType, Long snapshotEpochMillis, Pageable pageable) {
        if (snapshotEpochMillis == null) {
            return flipType == null
                    ? flipRepository.findAll(pageable)
                    : flipRepository.findAllByFlipType(flipType, pageable);
        }
        return flipType == null
                ? flipRepository.findAllBySnapshotTimestampEpochMillis(snapshotEpochMillis, pageable)
                : flipRepository.findAllByFlipTypeAndSnapshotTimestampEpochMillis(flipType, snapshotEpochMillis, pageable);
    }
}
