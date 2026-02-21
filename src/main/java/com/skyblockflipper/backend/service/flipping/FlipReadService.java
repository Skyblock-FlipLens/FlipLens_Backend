package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.FlipCoverageDto;
import com.skyblockflipper.backend.api.FlipGoodnessDto;
import com.skyblockflipper.backend.api.FlipSnapshotStatsDto;
import com.skyblockflipper.backend.api.FlipSortBy;
import com.skyblockflipper.backend.api.FlipTypesDto;
import com.skyblockflipper.backend.api.FlipSummaryStatsDto;
import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.repository.FlipRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.function.Function;

@Service
public class FlipReadService {

    private static final int GOODNESS_PAGE_SIZE = 10;
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

    public Page<UnifiedFlipDto> filterFlips(FlipType flipType,
                                            Instant snapshotTimestamp,
                                            Double minLiquidityScore,
                                            Double maxRiskScore,
                                            Long minExpectedProfit,
                                            Double minRoi,
                                            Double minRoiPerHour,
                                            Long maxRequiredCapital,
                                            Boolean partial,
                                            FlipSortBy sortBy,
                                            Sort.Direction sortDirection,
                                            Pageable pageable) {
        List<UnifiedFlipDto> mapped = filterFlipsAsList(
                flipType,
                snapshotTimestamp,
                minLiquidityScore,
                maxRiskScore,
                minExpectedProfit,
                minRoi,
                minRoiPerHour,
                maxRequiredCapital,
                partial,
                sortBy,
                sortDirection
        );
        return paginateUnified(mapped, pageable);
    }

    public List<UnifiedFlipDto> topFlips(FlipType flipType,
                                         Instant snapshotTimestamp,
                                         Double minLiquidityScore,
                                         Double maxRiskScore,
                                         Long minExpectedProfit,
                                         Double minRoi,
                                         Double minRoiPerHour,
                                         Long maxRequiredCapital,
                                         Boolean partial,
                                         int limit) {
        int safeLimit = Math.max(1, limit);
        return filterFlipsAsList(
                flipType,
                snapshotTimestamp,
                minLiquidityScore,
                maxRiskScore,
                minExpectedProfit,
                minRoi,
                minRoiPerHour,
                maxRequiredCapital,
                partial,
                FlipSortBy.EXPECTED_PROFIT,
                Sort.Direction.DESC
        ).stream().limit(safeLimit).toList();
    }

    public FlipSummaryStatsDto summaryStats(FlipType flipType, Instant snapshotTimestamp) {
        Long snapshotEpochMillis = resolveSnapshotEpochMillis(snapshotTimestamp);
        if (snapshotEpochMillis == null) {
            return new FlipSummaryStatsDto(0L, 0L, 0D, 0L, Map.of());
        }
        FlipCalculationContext context = snapshotEpochMillis == null
                ? flipCalculationContextService.loadCurrentContext()
                : flipCalculationContextService.loadContextAsOf(Instant.ofEpochMilli(snapshotEpochMillis));

        List<UnifiedFlipDto> mapped = queryFlips(flipType, snapshotEpochMillis, Pageable.unpaged())
                .stream()
                .map(flip -> unifiedFlipDtoMapper.toDto(flip, context))
                .filter(Objects::nonNull)
                .toList();
        long avgProfit = Math.round(mapped.stream()
                .map(UnifiedFlipDto::expectedProfit)
                .filter(value -> value != null && value > 0)
                .mapToLong(Long::longValue)
                .average().orElse(0D));
        double avgRoi = mapped.stream()
                .map(UnifiedFlipDto::roi)
                .filter(value -> value != null && !Double.isNaN(value) && !Double.isInfinite(value))
                .mapToDouble(Double::doubleValue)
                .average().orElse(0D);
        long bestFlipProfit = mapped.stream()
                .map(UnifiedFlipDto::expectedProfit)
                .filter(value -> value != null && value > 0)
                .max(Long::compareTo)
                .orElse(0L);

        Map<String, Long> byType = new LinkedHashMap<>();
        for (FlipType type : FlipType.values()) {
            long count = queryFlips(type, snapshotEpochMillis, Pageable.unpaged()).getTotalElements();
            if (flipType == null || flipType == type) {
                byType.put(type.name(), count);
            }
        }
        return new FlipSummaryStatsDto(mapped.size(), avgProfit, round2(avgRoi), bestFlipProfit, byType);
    }

    public Page<UnifiedFlipDto> topLiquidityFlips(FlipType flipType, Instant snapshotTimestamp, Pageable pageable) {
        return filterFlips(
                flipType,
                snapshotTimestamp,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                FlipSortBy.LIQUIDITY_SCORE,
                Sort.Direction.DESC,
                pageable
        );
    }

    public Page<UnifiedFlipDto> lowestRiskFlips(FlipType flipType, Instant snapshotTimestamp, Pageable pageable) {
        return filterFlips(
                flipType,
                snapshotTimestamp,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                FlipSortBy.RISK_SCORE,
                Sort.Direction.ASC,
                pageable
        );
    }

    public Page<FlipGoodnessDto> topGoodnessFlips(FlipType flipType, Instant snapshotTimestamp, int page) {
        Long snapshotEpochMillis = resolveSnapshotEpochMillis(snapshotTimestamp);
        FlipCalculationContext context = snapshotEpochMillis == null
                ? flipCalculationContextService.loadCurrentContext()
                : flipCalculationContextService.loadContextAsOf(Instant.ofEpochMilli(snapshotEpochMillis));

        List<FlipGoodnessDto> ranked = queryFlips(flipType, snapshotEpochMillis, Pageable.unpaged())
                .stream()
                .map(flip -> unifiedFlipDtoMapper.toDto(flip, context))
                .filter(Objects::nonNull)
                .map(this::toGoodnessDto)
                .sorted(Comparator.comparing(FlipGoodnessDto::goodnessScore, Comparator.reverseOrder())
                        .thenComparing(entry -> {
                            UnifiedFlipDto flip = entry.flip();
                            return flip.expectedProfit() == null ? Long.MIN_VALUE : flip.expectedProfit();
                        }, Comparator.reverseOrder())
                        .thenComparing(entry -> entry.flip().id() == null ? "" : entry.flip().id().toString()))
                .toList();

        int safePage = Math.max(0, page);
        return paginateGoodness(ranked, PageRequest.of(safePage, GOODNESS_PAGE_SIZE));
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

    public Optional<Long> latestSnapshotEpochMillis() {
        return flipRepository.findMaxSnapshotTimestampEpochMillis();
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

    private Comparator<UnifiedFlipDto> comparatorFor(FlipSortBy sortBy, Sort.Direction direction) {
        return switch (sortBy) {
            case ROI -> comparableComparator(UnifiedFlipDto::roi, direction);
            case ROI_PER_HOUR -> comparableComparator(UnifiedFlipDto::roiPerHour, direction);
            case LIQUIDITY_SCORE -> comparableComparator(UnifiedFlipDto::liquidityScore, direction);
            case RISK_SCORE -> comparableComparator(UnifiedFlipDto::riskScore, direction);
            case REQUIRED_CAPITAL -> comparableComparator(UnifiedFlipDto::requiredCapital, direction);
            case FEES -> comparableComparator(UnifiedFlipDto::fees, direction);
            case DURATION_SECONDS -> comparableComparator(UnifiedFlipDto::durationSeconds, direction);
            case EXPECTED_PROFIT -> comparableComparator(UnifiedFlipDto::expectedProfit, direction);
        };
    }

    private List<UnifiedFlipDto> filterFlipsAsList(FlipType flipType,
                                                   Instant snapshotTimestamp,
                                                   Double minLiquidityScore,
                                                   Double maxRiskScore,
                                                   Long minExpectedProfit,
                                                   Double minRoi,
                                                   Double minRoiPerHour,
                                                   Long maxRequiredCapital,
                                                   Boolean partial,
                                                   FlipSortBy sortBy,
                                                   Sort.Direction sortDirection) {
        Long snapshotEpochMillis = resolveSnapshotEpochMillis(snapshotTimestamp);
        FlipCalculationContext context = snapshotEpochMillis == null
                ? flipCalculationContextService.loadCurrentContext()
                : flipCalculationContextService.loadContextAsOf(Instant.ofEpochMilli(snapshotEpochMillis));

        return queryFlips(flipType, snapshotEpochMillis, Pageable.unpaged())
                .stream()
                .map(flip -> unifiedFlipDtoMapper.toDto(flip, context))
                .filter(Objects::nonNull)
                .filter(dto -> minLiquidityScore == null
                        || (dto.liquidityScore() != null && dto.liquidityScore() >= minLiquidityScore))
                .filter(dto -> maxRiskScore == null
                        || (dto.riskScore() != null && dto.riskScore() <= maxRiskScore))
                .filter(dto -> minExpectedProfit == null
                        || (dto.expectedProfit() != null && dto.expectedProfit() >= minExpectedProfit))
                .filter(dto -> minRoi == null
                        || (dto.roi() != null && dto.roi() >= minRoi))
                .filter(dto -> minRoiPerHour == null
                        || (dto.roiPerHour() != null && dto.roiPerHour() >= minRoiPerHour))
                .filter(dto -> maxRequiredCapital == null
                        || (dto.requiredCapital() != null && dto.requiredCapital() <= maxRequiredCapital))
                .filter(dto -> partial == null || dto.partial() == partial)
                .sorted(comparatorFor(sortBy == null ? FlipSortBy.EXPECTED_PROFIT : sortBy,
                        sortDirection == null ? Sort.Direction.DESC : sortDirection))
                .toList();
    }

    private <T extends Comparable<? super T>> Comparator<UnifiedFlipDto> comparableComparator(
            Function<UnifiedFlipDto, T> extractor,
            Sort.Direction direction
    ) {
        Comparator<T> valueComparator = direction == Sort.Direction.ASC
                ? Comparator.nullsLast(Comparator.naturalOrder())
                : Comparator.nullsLast(Comparator.reverseOrder());
        return Comparator.comparing(extractor, valueComparator)
                .thenComparing(dto -> dto.id() == null ? "" : dto.id().toString());
    }

    private Page<UnifiedFlipDto> paginateUnified(List<UnifiedFlipDto> dtos, Pageable pageable) {
        if (pageable == null || pageable.isUnpaged()) {
            return new PageImpl<>(dtos);
        }
        int fromIndex = (int) Math.min((long) pageable.getPageNumber() * pageable.getPageSize(), dtos.size());
        int toIndex = Math.min(fromIndex + pageable.getPageSize(), dtos.size());
        List<UnifiedFlipDto> pageContent = dtos.subList(fromIndex, toIndex);
        return new PageImpl<>(pageContent, pageable, dtos.size());
    }

    private Page<FlipGoodnessDto> paginateGoodness(List<FlipGoodnessDto> dtos, Pageable pageable) {
        if (pageable == null || pageable.isUnpaged()) {
            return new PageImpl<>(dtos);
        }
        int fromIndex = (int) Math.min((long) pageable.getPageNumber() * pageable.getPageSize(), dtos.size());
        int toIndex = Math.min(fromIndex + pageable.getPageSize(), dtos.size());
        List<FlipGoodnessDto> pageContent = dtos.subList(fromIndex, toIndex);
        return new PageImpl<>(pageContent, pageable, dtos.size());
    }

    private FlipGoodnessDto toGoodnessDto(UnifiedFlipDto dto) {
        double roiPerHourScore = roiPerHourScore(dto.roiPerHour());
        double profitScore = profitScore(dto.expectedProfit());
        double liquidityScore = clamp(nullableDouble(dto.liquidityScore()), 0D, 100D);
        double inverseRiskScore = 100D - clamp(nullableDouble(dto.riskScore()), 0D, 100D);
        boolean partialPenaltyApplied = dto.partial();

        double weighted = (0.35D * roiPerHourScore)
                + (0.25D * profitScore)
                + (0.25D * liquidityScore)
                + (0.15D * inverseRiskScore);
        if (partialPenaltyApplied) {
            weighted -= 10D;
        }
        double score = clamp(weighted, 0D, 100D);

        return new FlipGoodnessDto(
                dto,
                score,
                new FlipGoodnessDto.GoodnessBreakdown(
                        round2(roiPerHourScore),
                        round2(profitScore),
                        round2(liquidityScore),
                        round2(inverseRiskScore),
                        partialPenaltyApplied
                )
        );
    }

    private double roiPerHourScore(Double roiPerHour) {
        double value = Math.max(0D, nullableDouble(roiPerHour));
        return 100D * (1D - Math.exp(-2D * value));
    }

    private double profitScore(Long expectedProfit) {
        double value = Math.max(0D, expectedProfit == null ? 0D : expectedProfit.doubleValue());
        return Math.min(100D, Math.log10(value + 1D) * 10D);
    }

    private double nullableDouble(Double value) {
        if (value == null || Double.isNaN(value) || Double.isInfinite(value)) {
            return 0D;
        }
        return value;
    }

    private double round2(double value) {
        return Math.round(value * 100D) / 100D;
    }

    private double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }
}
