package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContext;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class FlipStorageParityService {

    private static final double DOUBLE_TOLERANCE = 1e-9D;

    private final FlipRepository flipRepository;
    private final FlipCalculationContextService flipCalculationContextService;
    private final UnifiedFlipDtoMapper unifiedFlipDtoMapper;
    private final UnifiedFlipCurrentReadService unifiedFlipCurrentReadService;
    private final FlipIdentityService flipIdentityService;
    private final FlipStorageProperties flipStorageProperties;

    public FlipStorageParityService(FlipRepository flipRepository,
                                    FlipCalculationContextService flipCalculationContextService,
                                    UnifiedFlipDtoMapper unifiedFlipDtoMapper,
                                    UnifiedFlipCurrentReadService unifiedFlipCurrentReadService,
                                    FlipIdentityService flipIdentityService,
                                    FlipStorageProperties flipStorageProperties) {
        this.flipRepository = flipRepository;
        this.flipCalculationContextService = flipCalculationContextService;
        this.unifiedFlipDtoMapper = unifiedFlipDtoMapper;
        this.unifiedFlipCurrentReadService = unifiedFlipCurrentReadService;
        this.flipIdentityService = flipIdentityService;
        this.flipStorageProperties = flipStorageProperties;
    }

    @Transactional(readOnly = true)
    public FlipStorageParityReport latestParityReport() {
        Long legacyLatestSnapshot = flipRepository.findMaxSnapshotTimestampEpochMillis().orElse(null);
        Long currentLatestSnapshot = unifiedFlipCurrentReadService.latestSnapshotEpochMillis().orElse(null);
        Long comparisonSnapshot = resolveComparisonSnapshot(legacyLatestSnapshot, currentLatestSnapshot);
        boolean snapshotAligned = legacyLatestSnapshot != null
                && currentLatestSnapshot != null
                && legacyLatestSnapshot.equals(currentLatestSnapshot);

        List<UnifiedFlipDto> legacyDtos = loadLegacyDtos(comparisonSnapshot);
        List<UnifiedFlipDto> currentDtos = loadCurrentDtos(comparisonSnapshot);

        Map<UUID, UnifiedFlipDto> legacyById = toIdMap(legacyDtos);
        Map<UUID, UnifiedFlipDto> currentById = toIdMap(currentDtos);

        List<UUID> onlyLegacyIds = legacyById.keySet().stream().filter(id -> !currentById.containsKey(id)).toList();
        List<UUID> onlyCurrentIds = currentById.keySet().stream().filter(id -> !legacyById.containsKey(id)).toList();
        List<UUID> sharedIds = legacyById.keySet().stream().filter(currentById::containsKey).toList();

        List<MetricMismatch> mismatches = new ArrayList<>();
        for (UUID id : sharedIds) {
            UnifiedFlipDto legacy = legacyById.get(id);
            UnifiedFlipDto current = currentById.get(id);
            List<String> fields = mismatchedFields(legacy, current);
            if (!fields.isEmpty()) {
                mismatches.add(new MetricMismatch(id, fields));
            }
        }

        int sampleSize = Math.max(1, flipStorageProperties.getParitySampleSize());
        List<String> sampleOnlyLegacy = onlyLegacyIds.stream()
                .limit(sampleSize)
                .map(UUID::toString)
                .toList();
        List<String> sampleOnlyCurrent = onlyCurrentIds.stream()
                .limit(sampleSize)
                .map(UUID::toString)
                .toList();
        List<MetricMismatch> sampleMismatches = mismatches.stream().limit(sampleSize).toList();

        Map<String, Long> legacyByType = countByType(legacyDtos);
        Map<String, Long> currentByType = countByType(currentDtos);

        boolean parityOk = snapshotAligned
                && onlyLegacyIds.isEmpty()
                && onlyCurrentIds.isEmpty()
                && mismatches.isEmpty();

        return new FlipStorageParityReport(
                Instant.now(),
                new Flags(
                        flipStorageProperties.isDualWriteEnabled(),
                        flipStorageProperties.isReadFromNew(),
                        flipStorageProperties.isLegacyWriteEnabled(),
                        flipStorageProperties.getTrendRelativeThreshold(),
                        flipStorageProperties.getTrendScoreDeltaThreshold(),
                        sampleSize
                ),
                comparisonSnapshot,
                legacyLatestSnapshot,
                currentLatestSnapshot,
                snapshotAligned,
                legacyById.size(),
                currentById.size(),
                sharedIds.size(),
                onlyLegacyIds.size(),
                onlyCurrentIds.size(),
                mismatches.size(),
                parityOk,
                legacyByType,
                currentByType,
                sampleOnlyLegacy,
                sampleOnlyCurrent,
                sampleMismatches
        );
    }

    private Long resolveComparisonSnapshot(Long legacyLatestSnapshot, Long currentLatestSnapshot) {
        if (legacyLatestSnapshot == null && currentLatestSnapshot == null) {
            return null;
        }
        if (legacyLatestSnapshot != null && currentLatestSnapshot != null) {
            if (legacyLatestSnapshot.equals(currentLatestSnapshot)) {
                return legacyLatestSnapshot;
            }
            if (flipRepository.existsBySnapshotTimestampEpochMillis(currentLatestSnapshot)) {
                return currentLatestSnapshot;
            }
            return legacyLatestSnapshot;
        }
        return legacyLatestSnapshot != null ? legacyLatestSnapshot : currentLatestSnapshot;
    }

    private List<UnifiedFlipDto> loadLegacyDtos(Long comparisonSnapshot) {
        if (comparisonSnapshot == null) {
            return List.of();
        }
        List<Flip> legacyFlips = flipRepository.findAllBySnapshotTimestampEpochMillis(comparisonSnapshot);
        if (legacyFlips.isEmpty()) {
            return List.of();
        }
        FlipCalculationContext context = flipCalculationContextService.loadContextAsOf(Instant.ofEpochMilli(comparisonSnapshot));
        return legacyFlips.stream()
                .map(flip -> {
                    UnifiedFlipDto dto = unifiedFlipDtoMapper.toDto(flip, context);
                    if (dto == null) {
                        return null;
                    }
                    UUID stableId = flipIdentityService.derive(flip).stableFlipId();
                    return withStableId(dto, stableId);
                })
                .filter(dto -> dto != null)
                .toList();
    }

    private List<UnifiedFlipDto> loadCurrentDtos(Long comparisonSnapshot) {
        List<UnifiedFlipDto> currentDtos = unifiedFlipCurrentReadService.listCurrent(null);
        if (comparisonSnapshot == null) {
            return currentDtos;
        }
        Instant snapshotTimestamp = Instant.ofEpochMilli(comparisonSnapshot);
        return currentDtos.stream()
                .filter(dto -> snapshotTimestamp.equals(dto.snapshotTimestamp()))
                .toList();
    }

    private Map<UUID, UnifiedFlipDto> toIdMap(List<UnifiedFlipDto> dtos) {
        Map<UUID, UnifiedFlipDto> byId = new LinkedHashMap<>();
        for (UnifiedFlipDto dto : dtos) {
            if (dto == null || dto.id() == null) {
                continue;
            }
            byId.putIfAbsent(dto.id(), dto);
        }
        return byId;
    }

    private Map<String, Long> countByType(List<UnifiedFlipDto> dtos) {
        EnumMap<FlipType, Long> counts = new EnumMap<>(FlipType.class);
        for (FlipType type : FlipType.values()) {
            counts.put(type, 0L);
        }
        for (UnifiedFlipDto dto : dtos) {
            if (dto == null || dto.flipType() == null) {
                continue;
            }
            counts.computeIfPresent(dto.flipType(), (key, value) -> value + 1L);
        }
        Map<String, Long> sorted = new LinkedHashMap<>();
        counts.entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().name()))
                .forEach(entry -> sorted.put(entry.getKey().name(), entry.getValue()));
        return sorted;
    }

    private List<String> mismatchedFields(UnifiedFlipDto legacy, UnifiedFlipDto current) {
        List<String> mismatches = new ArrayList<>();
        if (!equalsLong(legacy.expectedProfit(), current.expectedProfit())) {
            mismatches.add("expectedProfit");
        }
        if (!equalsLong(legacy.requiredCapital(), current.requiredCapital())) {
            mismatches.add("requiredCapital");
        }
        if (!equalsLong(legacy.durationSeconds(), current.durationSeconds())) {
            mismatches.add("durationSeconds");
        }
        if (!equalsLong(legacy.fees(), current.fees())) {
            mismatches.add("fees");
        }
        if (!equalsDouble(legacy.roi(), current.roi())) {
            mismatches.add("roi");
        }
        if (!equalsDouble(legacy.roiPerHour(), current.roiPerHour())) {
            mismatches.add("roiPerHour");
        }
        if (!equalsDouble(legacy.liquidityScore(), current.liquidityScore())) {
            mismatches.add("liquidityScore");
        }
        if (!equalsDouble(legacy.riskScore(), current.riskScore())) {
            mismatches.add("riskScore");
        }
        if (legacy.partial() != current.partial()) {
            mismatches.add("partial");
        }
        return mismatches;
    }

    private boolean equalsLong(Long left, Long right) {
        return Optional.ofNullable(left).orElse(0L).equals(Optional.ofNullable(right).orElse(0L));
    }

    private boolean equalsDouble(Double left, Double right) {
        double leftValue = left == null ? 0D : left;
        double rightValue = right == null ? 0D : right;
        return Math.abs(leftValue - rightValue) <= DOUBLE_TOLERANCE;
    }

    private UnifiedFlipDto withStableId(UnifiedFlipDto dto, UUID stableId) {
        return new UnifiedFlipDto(
                stableId,
                dto.flipType(),
                dto.inputItems(),
                dto.outputItems(),
                dto.requiredCapital(),
                dto.expectedProfit(),
                dto.roi(),
                dto.roiPerHour(),
                dto.durationSeconds(),
                dto.fees(),
                dto.liquidityScore(),
                dto.riskScore(),
                dto.snapshotTimestamp(),
                dto.partial(),
                dto.partialReasons(),
                dto.steps(),
                dto.constraints()
        );
    }

    public record FlipStorageParityReport(
            Instant generatedAt,
            Flags flags,
            Long comparisonSnapshotEpochMillis,
            Long legacyLatestSnapshotEpochMillis,
            Long currentLatestSnapshotEpochMillis,
            boolean snapshotAligned,
            long legacyCount,
            long currentCount,
            long intersectionCount,
            long onlyLegacyCount,
            long onlyCurrentCount,
            long metricMismatchCount,
            boolean parityOk,
            Map<String, Long> legacyByType,
            Map<String, Long> currentByType,
            List<String> sampleOnlyLegacyIds,
            List<String> sampleOnlyCurrentIds,
            List<MetricMismatch> sampleMetricMismatches
    ) {
    }

    public record Flags(
            boolean dualWriteEnabled,
            boolean readFromNew,
            boolean legacyWriteEnabled,
            double trendRelativeThreshold,
            double trendScoreDeltaThreshold,
            int paritySampleSize
    ) {
    }

    public record MetricMismatch(
            UUID flipId,
            List<String> mismatchedFields
    ) {
    }
}
