package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.FlipCoverageDto;
import com.skyblockflipper.backend.api.FlipGoodnessDto;
import com.skyblockflipper.backend.api.FlipSnapshotStatsDto;
import com.skyblockflipper.backend.api.FlipSortBy;
import com.skyblockflipper.backend.api.FlipTypesDto;
import com.skyblockflipper.backend.api.FlipSummaryStatsDto;
import com.skyblockflipper.backend.api.OffsetLimitPageRequest;
import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.config.properties.FlippingModelProperties;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.storage.FlipStorageProperties;
import com.skyblockflipper.backend.service.flipping.storage.OnDemandFlipSnapshotService;
import com.skyblockflipper.backend.service.flipping.storage.UnifiedFlipCurrentReadService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
public class FlipReadService {

    private static final int GOODNESS_PAGE_SIZE = 10;
    private static final int GOODNESS_CACHE_MAX_ENTRIES = 8;
    private static final long GOODNESS_CACHE_TTL_MILLIS = 15_000L;
    private static final String MISSING_INPUT_PRICE_REASON_PREFIX = "MISSING_INPUT_PRICE";
    private static final String INSUFFICIENT_INPUT_DEPTH_REASON_PREFIX = "INSUFFICIENT_INPUT_DEPTH";
    private static final String MISSING_OUTPUT_PRICE_REASON_PREFIX = "MISSING_OUTPUT_PRICE";
    private static final String INSUFFICIENT_OUTPUT_DEPTH_REASON_PREFIX = "INSUFFICIENT_OUTPUT_DEPTH";
    private static final String AMBIGUOUS_INPUT_SOURCE_REASON_PREFIX = "AMBIGUOUS_INPUT_MARKET_SOURCE";
    private static final String MISSING_ELECTION_DATA_REASON = "MISSING_ELECTION_DATA";
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
    private final UnifiedFlipCurrentReadService unifiedFlipCurrentReadService;
    private final OnDemandFlipSnapshotService onDemandFlipSnapshotService;
    private final FlipStorageProperties flipStorageProperties;
    private final FlippingModelProperties flippingModelProperties;
    private final ConcurrentMap<GoodnessCacheKey, CachedGoodnessRanking> goodnessRankingCache = new ConcurrentHashMap<>();

    public FlipReadService(FlipRepository flipRepository,
                           UnifiedFlipDtoMapper unifiedFlipDtoMapper,
                           FlipCalculationContextService flipCalculationContextService) {
        this(flipRepository, unifiedFlipDtoMapper, flipCalculationContextService, null, null, null, null);
    }

    public FlipReadService(FlipRepository flipRepository,
                           UnifiedFlipDtoMapper unifiedFlipDtoMapper,
                           FlipCalculationContextService flipCalculationContextService,
                           UnifiedFlipCurrentReadService unifiedFlipCurrentReadService,
                           OnDemandFlipSnapshotService onDemandFlipSnapshotService,
                           FlipStorageProperties flipStorageProperties) {
        this(
                flipRepository,
                unifiedFlipDtoMapper,
                flipCalculationContextService,
                unifiedFlipCurrentReadService,
                onDemandFlipSnapshotService,
                flipStorageProperties,
                null
        );
    }

    @Autowired
    public FlipReadService(FlipRepository flipRepository,
                           UnifiedFlipDtoMapper unifiedFlipDtoMapper,
                           FlipCalculationContextService flipCalculationContextService,
                           UnifiedFlipCurrentReadService unifiedFlipCurrentReadService,
                           OnDemandFlipSnapshotService onDemandFlipSnapshotService,
                           FlipStorageProperties flipStorageProperties,
                           FlippingModelProperties flippingModelProperties) {
        this.flipRepository = flipRepository;
        this.unifiedFlipDtoMapper = unifiedFlipDtoMapper;
        this.flipCalculationContextService = flipCalculationContextService;
        this.unifiedFlipCurrentReadService = unifiedFlipCurrentReadService;
        this.onDemandFlipSnapshotService = onDemandFlipSnapshotService;
        this.flipStorageProperties = flipStorageProperties;
        this.flippingModelProperties = flippingModelProperties == null ? new FlippingModelProperties() : flippingModelProperties;
    }

    public Page<UnifiedFlipDto> listFlips(FlipType flipType, Pageable pageable) {
        return listFlips(flipType, null, pageable);
    }

    public Page<UnifiedFlipDto> listFlips(FlipType flipType, Instant snapshotTimestamp, Pageable pageable) {
        if (useCurrentStorage(snapshotTimestamp)) {
            Pageable normalized = normalizeCurrentStoragePageable(pageable);
            Page<UnifiedFlipDto> page = unifiedFlipCurrentReadService.listCurrentPage(flipType, normalized);
            return page.map(dto -> dto);
        }
        if (useOnDemandSnapshot(snapshotTimestamp)) {
            List<UnifiedFlipDto> onDemand = sortedById(onDemandFlipSnapshotService.computeSnapshotDtos(snapshotTimestamp, flipType));
            return paginateUnified(onDemand, pageable);
        }

        Long snapshotEpochMillis = resolveSnapshotEpochMillis(snapshotTimestamp);
        FlipCalculationContext context = snapshotEpochMillis == null
                ? flipCalculationContextService.loadCurrentContext()
                : flipCalculationContextService.loadContextAsOf(Instant.ofEpochMilli(snapshotEpochMillis));

        Page<Flip> flips = queryFlips(flipType, snapshotEpochMillis, pageable);
        return flips.map(flip -> unifiedFlipDtoMapper.toDto(flip, context));
    }

    public Optional<UnifiedFlipDto> findFlipById(UUID id) {
        if (isReadFromNewEnabled() && unifiedFlipCurrentReadService != null) {
            Optional<UnifiedFlipDto> fromCurrent = unifiedFlipCurrentReadService.findByStableFlipId(id);
            if (fromCurrent.isPresent()) {
                return fromCurrent;
            }
        }

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
        if (useCurrentStorage(snapshotTimestamp)) {
            List<UnifiedFlipDto> ranked = applyFiltersAndSort(
                    unifiedFlipCurrentReadService.listCurrentScoringDtos(flipType),
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
            return hydrateCurrentUnifiedPage(paginateUnified(ranked, pageable));
        }

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
        if (useCurrentStorage(snapshotTimestamp)) {
            List<UnifiedFlipDto> ranked = applyFiltersAndSort(
                    unifiedFlipCurrentReadService.listCurrentScoringDtos(flipType),
                    minLiquidityScore,
                    maxRiskScore,
                    minExpectedProfit,
                    minRoi,
                    minRoiPerHour,
                    maxRequiredCapital,
                    partial,
                    FlipSortBy.EXPECTED_PROFIT,
                    Sort.Direction.DESC
            );
            return hydrateCurrentUnifiedList(ranked.stream().limit(safeLimit).toList());
        }
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
        if (useCurrentStorage(snapshotTimestamp)) {
            List<UnifiedFlipDto> mapped = unifiedFlipCurrentReadService.listCurrentScoringDtos(flipType);
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
            Map<FlipType, Long> counts = unifiedFlipCurrentReadService.countsByType();
            for (FlipType type : FlipType.values()) {
                long count = counts.getOrDefault(type, 0L);
                if (flipType == null || flipType == type) {
                    byType.put(type.name(), count);
                }
            }
            return new FlipSummaryStatsDto(mapped.size(), avgProfit, round2(avgRoi), bestFlipProfit, byType);
        }
        if (useOnDemandSnapshot(snapshotTimestamp)) {
            List<UnifiedFlipDto> allTypes = onDemandFlipSnapshotService.computeSnapshotDtos(snapshotTimestamp, null);
            List<UnifiedFlipDto> mapped = flipType == null
                    ? allTypes
                    : allTypes.stream().filter(dto -> dto.flipType() == flipType).toList();
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

            EnumMap<FlipType, Long> countsByType = new EnumMap<>(FlipType.class);
            for (FlipType type : FlipType.values()) {
                countsByType.put(type, 0L);
            }
            for (UnifiedFlipDto dto : allTypes) {
                if (dto == null || dto.flipType() == null) {
                    continue;
                }
                countsByType.computeIfPresent(dto.flipType(), (key, count) -> count + 1L);
            }

            Map<String, Long> byType = new LinkedHashMap<>();
            for (FlipType type : FlipType.values()) {
                if (flipType == null || flipType == type) {
                    byType.put(type.name(), countsByType.getOrDefault(type, 0L));
                }
            }
            return new FlipSummaryStatsDto(mapped.size(), avgProfit, round2(avgRoi), bestFlipProfit, byType);
        }

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

        EnumMap<FlipType, Long> countsByType = new EnumMap<>(FlipType.class);
        for (FlipType type : FlipType.values()) {
            countsByType.put(type, 0L);
        }
        for (Object[] row : flipRepository.countByFlipTypeForSnapshot(snapshotEpochMillis)) {
            if (row == null || row.length < 2 || !(row[0] instanceof FlipType type)) {
                continue;
            }
            countsByType.put(type, toLong(row[1]));
        }

        Map<String, Long> byType = new LinkedHashMap<>();
        for (FlipType type : FlipType.values()) {
            if (flipType == null || flipType == type) {
                byType.put(type.name(), countsByType.getOrDefault(type, 0L));
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
        int safePage = Math.max(0, page);
        return topGoodnessFlips(flipType, snapshotTimestamp, PageRequest.of(safePage, GOODNESS_PAGE_SIZE));
    }

    public Page<FlipGoodnessDto> topGoodnessFlips(FlipType flipType, Instant snapshotTimestamp, Pageable pageable) {
        Sort sort = Sort.unsorted();
        Pageable fixedPageable = PageRequest.of(0, GOODNESS_PAGE_SIZE, sort);
        boolean usingCurrentStorage = useCurrentStorage(snapshotTimestamp);
        if (pageable != null) {
            sort = pageable.getSort() == null ? Sort.unsorted() : pageable.getSort();
            fixedPageable = pageable.isUnpaged()
                    ? PageRequest.of(0, GOODNESS_PAGE_SIZE, sort)
                    : OffsetLimitPageRequest.of(Math.max(0L, pageable.getOffset()), GOODNESS_PAGE_SIZE, sort);
        }

        List<FlipGoodnessDto> ranked;
        if (usingCurrentStorage) {
            Long snapshotEpochMillis = unifiedFlipCurrentReadService.latestSnapshotEpochMillis().orElse(null);
            ranked = cachedGoodnessRanking(
                    flipType,
                    snapshotEpochMillis,
                    () -> rankByGoodness(unifiedFlipCurrentReadService.listCurrentScoringDtos(flipType))
            );
        } else if (useOnDemandSnapshot(snapshotTimestamp)) {
            ranked = rankByGoodness(onDemandFlipSnapshotService.computeSnapshotDtos(snapshotTimestamp, flipType));
        } else {
            Long snapshotEpochMillis = resolveSnapshotEpochMillis(snapshotTimestamp);
            ranked = cachedGoodnessRanking(
                    flipType,
                    snapshotEpochMillis,
                    () -> computeGoodnessRanking(flipType, snapshotEpochMillis)
            );
        }

        Page<FlipGoodnessDto> paged = paginateGoodness(ranked, fixedPageable);
        if (!usingCurrentStorage || paged.isEmpty()) {
            return paged;
        }

        List<UUID> orderedIds = paged.getContent().stream()
                .map(FlipGoodnessDto::flip)
                .filter(Objects::nonNull)
                .map(UnifiedFlipDto::id)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        if (orderedIds.isEmpty()) {
            return paged;
        }

        Map<UUID, UnifiedFlipDto> fullById = new LinkedHashMap<>();
        for (UnifiedFlipDto full : unifiedFlipCurrentReadService.listCurrentByStableFlipIds(orderedIds)) {
            if (full != null && full.id() != null) {
                fullById.put(full.id(), full);
            }
        }

        List<FlipGoodnessDto> hydrated = paged.getContent().stream()
                .map(entry -> {
                    UnifiedFlipDto rankedFlip = entry.flip();
                    UUID stableId = rankedFlip == null ? null : rankedFlip.id();
                    UnifiedFlipDto full = stableId == null ? null : fullById.get(stableId);
                    if (full == null) {
                        return entry;
                    }
                    return new FlipGoodnessDto(full, entry.goodnessScore(), entry.breakdown());
                })
                .toList();

        return new PageImpl<>(hydrated, paged.getPageable(), paged.getTotalElements());
    }

    public FlipTypesDto listSupportedFlipTypes() {
        List<FlipType> flipTypes = Arrays.stream(FlipType.values())
                .sorted(Comparator.comparing(Enum::name))
                .toList();
        return new FlipTypesDto(flipTypes);
    }

    public FlipSnapshotStatsDto snapshotStats(Instant snapshotTimestamp) {
        if (useCurrentStorage(snapshotTimestamp)) {
            Optional<Long> latest = unifiedFlipCurrentReadService.latestSnapshotEpochMillis();
            if (latest.isEmpty()) {
                return new FlipSnapshotStatsDto(null, 0L, emptyTypeCounts());
            }
            Map<FlipType, Long> counts = unifiedFlipCurrentReadService.countsByType();
            long total = counts.values().stream().mapToLong(Long::longValue).sum();
            List<FlipSnapshotStatsDto.FlipTypeCountDto> byType = counts.entrySet().stream()
                    .sorted(Comparator.comparing(entry -> entry.getKey().name()))
                    .map(entry -> new FlipSnapshotStatsDto.FlipTypeCountDto(entry.getKey(), entry.getValue()))
                    .toList();
            return new FlipSnapshotStatsDto(Instant.ofEpochMilli(latest.get()), total, byType);
        }
        if (useOnDemandSnapshot(snapshotTimestamp)) {
            List<UnifiedFlipDto> dtos = onDemandFlipSnapshotService.computeSnapshotDtos(snapshotTimestamp, null);
            EnumMap<FlipType, Long> countsByType = new EnumMap<>(FlipType.class);
            for (FlipType type : FlipType.values()) {
                countsByType.put(type, 0L);
            }
            for (UnifiedFlipDto dto : dtos) {
                if (dto == null || dto.flipType() == null) {
                    continue;
                }
                countsByType.computeIfPresent(dto.flipType(), (key, value) -> value + 1L);
            }
            List<FlipSnapshotStatsDto.FlipTypeCountDto> byType = countsByType.entrySet().stream()
                    .sorted(Comparator.comparing(entry -> entry.getKey().name()))
                    .map(entry -> new FlipSnapshotStatsDto.FlipTypeCountDto(entry.getKey(), entry.getValue()))
                    .toList();
            return new FlipSnapshotStatsDto(snapshotTimestamp, dtos.size(), byType);
        }

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

        if (useCurrentStorage(null)) {
            Map<FlipType, Long> counts = unifiedFlipCurrentReadService.countsByType();
            for (FlipType flipType : COVERED_FLIP_TYPES) {
                countsByType.put(flipType, counts.getOrDefault(flipType, 0L));
            }
        } else if (snapshotEpochMillis != null) {
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
        if (isReadFromNewEnabled() && unifiedFlipCurrentReadService != null) {
            Optional<Long> latestCurrentSnapshot = unifiedFlipCurrentReadService.latestSnapshotEpochMillis();
            if (latestCurrentSnapshot.isPresent()) {
                return latestCurrentSnapshot.get();
            }
        }
        Optional<Long> latestSnapshot = flipRepository.findMaxSnapshotTimestampEpochMillis();
        return latestSnapshot.orElse(null);
    }

    public Optional<Long> latestSnapshotEpochMillis() {
        if (isReadFromNewEnabled() && unifiedFlipCurrentReadService != null) {
            Optional<Long> latestCurrentSnapshot = unifiedFlipCurrentReadService.latestSnapshotEpochMillis();
            if (latestCurrentSnapshot.isPresent()) {
                return latestCurrentSnapshot;
            }
        }
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
        if (useCurrentStorage(snapshotTimestamp)) {
            return applyFiltersAndSort(
                    unifiedFlipCurrentReadService.listCurrent(flipType),
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
        }
        if (useOnDemandSnapshot(snapshotTimestamp)) {
            return applyFiltersAndSort(
                    onDemandFlipSnapshotService.computeSnapshotDtos(snapshotTimestamp, flipType),
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
        }

        Long snapshotEpochMillis = resolveSnapshotEpochMillis(snapshotTimestamp);
        FlipCalculationContext context = snapshotEpochMillis == null
                ? flipCalculationContextService.loadCurrentContext()
                : flipCalculationContextService.loadContextAsOf(Instant.ofEpochMilli(snapshotEpochMillis));

        List<UnifiedFlipDto> mapped = queryFlips(flipType, snapshotEpochMillis, Pageable.unpaged())
                .stream()
                .map(flip -> unifiedFlipDtoMapper.toDto(flip, context))
                .toList();
        return applyFiltersAndSort(
                mapped,
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
    }

    private List<FlipGoodnessDto> cachedGoodnessRanking(FlipType flipType,
                                                        Long snapshotEpochMillis,
                                                        Supplier<List<FlipGoodnessDto>> computer) {
        if (snapshotEpochMillis == null) {
            return computer.get();
        }

        GoodnessCacheKey cacheKey = GoodnessCacheKey.from(snapshotEpochMillis, flipType, flippingModelProperties);
        long now = System.currentTimeMillis();
        CachedGoodnessRanking cached = goodnessRankingCache.get(cacheKey);
        if (cached != null && (now - cached.createdAtEpochMillis()) <= GOODNESS_CACHE_TTL_MILLIS) {
            return cached.ranking();
        }

        List<FlipGoodnessDto> computed = computer.get();
        goodnessRankingCache.put(cacheKey, new CachedGoodnessRanking(computed, now));
        trimGoodnessRankingCache();
        return computed;
    }

    private List<FlipGoodnessDto> computeGoodnessRanking(FlipType flipType, Long snapshotEpochMillis) {
        FlipCalculationContext context = snapshotEpochMillis == null
                ? flipCalculationContextService.loadCurrentContext()
                : flipCalculationContextService.loadContextAsOf(Instant.ofEpochMilli(snapshotEpochMillis));

        List<UnifiedFlipDto> mapped = queryFlips(flipType, snapshotEpochMillis, Pageable.unpaged())
                .stream()
                .map(flip -> unifiedFlipDtoMapper.toDto(flip, context))
                .filter(Objects::nonNull)
                .toList();
        return rankByGoodness(mapped);
    }

    private void trimGoodnessRankingCache() {
        if (goodnessRankingCache.size() <= GOODNESS_CACHE_MAX_ENTRIES) {
            return;
        }
        GoodnessCacheKey oldestKey = null;
        long oldestTimestamp = Long.MAX_VALUE;
        for (Map.Entry<GoodnessCacheKey, CachedGoodnessRanking> entry : goodnessRankingCache.entrySet()) {
            long createdAt = entry.getValue().createdAtEpochMillis();
            if (createdAt < oldestTimestamp) {
                oldestTimestamp = createdAt;
                oldestKey = entry.getKey();
            }
        }
        if (oldestKey != null) {
            goodnessRankingCache.remove(oldestKey);
        }
    }

    private boolean useCurrentStorage(Instant snapshotTimestamp) {
        return snapshotTimestamp == null && isReadFromNewEnabled() && unifiedFlipCurrentReadService != null;
    }

    private boolean isReadFromNewEnabled() {
        return flipStorageProperties != null && flipStorageProperties.isReadFromNew();
    }

    private boolean useOnDemandSnapshot(Instant snapshotTimestamp) {
        return snapshotTimestamp != null
                && isReadFromNewEnabled()
                && !isLegacyWriteEnabled()
                && onDemandFlipSnapshotService != null;
    }

    private boolean isLegacyWriteEnabled() {
        return flipStorageProperties == null || flipStorageProperties.isLegacyWriteEnabled();
    }

    private List<UnifiedFlipDto> sortedById(List<UnifiedFlipDto> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        return values.stream()
                .filter(this::isActionableFlip)
                .sorted(Comparator.comparing(dto -> dto.id() == null ? "" : dto.id().toString()))
                .toList();
    }

    private Page<UnifiedFlipDto> hydrateCurrentUnifiedPage(Page<UnifiedFlipDto> paged) {
        if (paged == null || paged.isEmpty()) {
            return paged == null ? Page.empty() : paged;
        }
        List<UnifiedFlipDto> hydrated = hydrateCurrentUnifiedList(paged.getContent());
        return new PageImpl<>(hydrated, paged.getPageable(), paged.getTotalElements());
    }

    private List<UnifiedFlipDto> hydrateCurrentUnifiedList(List<UnifiedFlipDto> ranked) {
        if (ranked == null || ranked.isEmpty()) {
            return List.of();
        }

        List<UUID> orderedIds = ranked.stream()
                .map(UnifiedFlipDto::id)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        if (orderedIds.isEmpty()) {
            return ranked;
        }

        Map<UUID, UnifiedFlipDto> fullById = new LinkedHashMap<>();
        for (UnifiedFlipDto full : unifiedFlipCurrentReadService.listCurrentByStableFlipIds(orderedIds)) {
            if (full != null && full.id() != null) {
                fullById.put(full.id(), full);
            }
        }

        List<UnifiedFlipDto> hydrated = new ArrayList<>(ranked.size());
        for (UnifiedFlipDto entry : ranked) {
            if (entry == null || entry.id() == null) {
                continue;
            }
            UnifiedFlipDto full = fullById.get(entry.id());
            hydrated.add(full == null ? entry : full);
        }
        return List.copyOf(hydrated);
    }

    private Pageable normalizeCurrentStoragePageable(Pageable pageable) {
        if (pageable == null || pageable.isUnpaged()) {
            return pageable;
        }
        Sort normalizedSort = Sort.unsorted();
        if (pageable.getSort() != null && pageable.getSort().isSorted()) {
            List<Sort.Order> normalizedOrders = new ArrayList<>();
            for (Sort.Order order : pageable.getSort()) {
                if (order == null) {
                    continue;
                }
                String property = order.getProperty();
                String normalizedProperty = "id".equals(property) ? "stableFlipId" : property;
                normalizedOrders.add(new Sort.Order(order.getDirection(), normalizedProperty));
            }
            if (!normalizedOrders.isEmpty()) {
                normalizedSort = Sort.by(normalizedOrders);
            }
        }
        return OffsetLimitPageRequest.of(pageable.getOffset(), pageable.getPageSize(), normalizedSort);
    }

    private List<UnifiedFlipDto> applyFiltersAndSort(List<UnifiedFlipDto> source,
                                                     Double minLiquidityScore,
                                                     Double maxRiskScore,
                                                     Long minExpectedProfit,
                                                     Double minRoi,
                                                     Double minRoiPerHour,
                                                     Long maxRequiredCapital,
                                                     Boolean partial,
                                                     FlipSortBy sortBy,
                                                     Sort.Direction sortDirection) {
        return (source == null ? List.<UnifiedFlipDto>of() : source).stream()
                .filter(Objects::nonNull)
                .filter(this::isActionableFlip)
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

    private List<FlipGoodnessDto> rankByGoodness(List<UnifiedFlipDto> source) {
        return (source == null ? List.<UnifiedFlipDto>of() : source).stream()
                .filter(Objects::nonNull)
                .filter(this::isActionableFlip)
                .map(this::toGoodnessDto)
                .sorted(Comparator.comparing(FlipGoodnessDto::goodnessScore, Comparator.reverseOrder())
                        .thenComparing(entry -> {
                            UnifiedFlipDto flip = entry.flip();
                            return flip.expectedProfit() == null ? Long.MIN_VALUE : flip.expectedProfit();
                        }, Comparator.reverseOrder())
                        .thenComparing(entry -> entry.flip().id() == null ? "" : entry.flip().id().toString()))
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
        int fromIndex = (int) Math.min(pageable.getOffset(), dtos.size());
        int toIndex = Math.min(fromIndex + pageable.getPageSize(), dtos.size());
        List<UnifiedFlipDto> pageContent = dtos.subList(fromIndex, toIndex);
        return new PageImpl<>(pageContent, pageable, dtos.size());
    }

    private Page<FlipGoodnessDto> paginateGoodness(List<FlipGoodnessDto> dtos, Pageable pageable) {
        if (pageable == null || pageable.isUnpaged()) {
            return new PageImpl<>(dtos);
        }
        int fromIndex = (int) Math.min(pageable.getOffset(), dtos.size());
        int toIndex = Math.min(fromIndex + pageable.getPageSize(), dtos.size());
        List<FlipGoodnessDto> pageContent = dtos.subList(fromIndex, toIndex);
        return new PageImpl<>(pageContent, pageable, dtos.size());
    }

    private FlipGoodnessDto toGoodnessDto(UnifiedFlipDto dto) {
        double roiPerHourScore = roiPerHourScore(dto.roiPerHour());
        double profitScore = profitScore(dto.expectedProfit());
        double liquidityScore = clamp(nullableDouble(dto.liquidityScore()), 0D, 100D);
        double inverseRiskScore = 100D - clamp(nullableDouble(dto.riskScore()), 0D, 100D);
        double confidenceScore = confidenceScore(dto);
        boolean partialPenaltyApplied = shouldApplyPartialPenalty(dto);

        double weighted;
        if (flippingModelProperties.isScoringV2Enabled()) {
            weighted = (0.45D * roiPerHourScore)
                    + (0.25D * liquidityScore)
                    + (0.15D * inverseRiskScore)
                    + (0.15D * confidenceScore);
        } else {
            weighted = (0.35D * roiPerHourScore)
                    + (0.25D * profitScore)
                    + (0.25D * liquidityScore)
                    + (0.15D * inverseRiskScore);
        }
        if (partialPenaltyApplied && !flippingModelProperties.isScoringV2Enabled()) {
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

    private boolean isActionableFlip(UnifiedFlipDto dto) {
        if (dto == null) {
            return false;
        }
        List<String> partialReasons = dto.partialReasons();
        if (partialReasons != null && !partialReasons.isEmpty()) {
            for (String reason : partialReasons) {
                if (reason != null
                        && (reason.startsWith(MISSING_INPUT_PRICE_REASON_PREFIX)
                        || reason.startsWith(INSUFFICIENT_INPUT_DEPTH_REASON_PREFIX))) {
                    return false;
                }
            }
        }
        if (!flippingModelProperties.isRecommendationGatesEnabled()) {
            return true;
        }
        long minExpectedProfit = Math.max(0L, flippingModelProperties.getMinRecommendationExpectedProfit());
        if (dto.expectedProfit() == null || dto.expectedProfit() <= minExpectedProfit) {
            return false;
        }
        double minLiquidity = Math.max(0D, flippingModelProperties.getMinRecommendationLiquidityScore());
        if (dto.liquidityScore() == null || dto.liquidityScore() < minLiquidity) {
            return false;
        }
        return confidenceScore(dto) >= Math.max(0D, flippingModelProperties.getMinConfidenceScore());
    }

    private double confidenceScore(UnifiedFlipDto dto) {
        if (dto == null) {
            return 0D;
        }
        double score = 100D;
        if (dto.partial()) {
            score -= 15D;
        }

        List<String> partialReasons = dto.partialReasons();
        if (partialReasons != null) {
            for (String reason : partialReasons) {
                if (reason == null || reason.isBlank()) {
                    continue;
                }
                if (reason.startsWith(MISSING_INPUT_PRICE_REASON_PREFIX)
                        || reason.startsWith(INSUFFICIENT_INPUT_DEPTH_REASON_PREFIX)) {
                    score -= 60D;
                    continue;
                }
                if (reason.startsWith(MISSING_OUTPUT_PRICE_REASON_PREFIX)
                        || reason.startsWith(INSUFFICIENT_OUTPUT_DEPTH_REASON_PREFIX)) {
                    score -= 30D;
                    continue;
                }
                if (reason.startsWith(AMBIGUOUS_INPUT_SOURCE_REASON_PREFIX)) {
                    score -= 25D;
                    continue;
                }
                if (MISSING_ELECTION_DATA_REASON.equals(reason)) {
                    score -= 5D;
                    continue;
                }
                score -= 10D;
            }
        }

        double liquidityScore = clamp(nullableDouble(dto.liquidityScore()), 0D, 100D);
        score += ((liquidityScore - 50D) * 0.2D);

        double riskScore = clamp(nullableDouble(dto.riskScore()), 0D, 100D);
        if (riskScore > 60D) {
            score -= (riskScore - 60D) * 0.3D;
        }

        if (flippingModelProperties.isOutlierProtectionEnabled()) {
            double roiPerHour = Math.max(0D, nullableDouble(dto.roiPerHour()));
            if (roiPerHour > 50D && (dto.partial() || liquidityScore < 40D)) {
                score -= 20D;
            }
            long expectedProfit = dto.expectedProfit() == null ? 0L : Math.max(0L, dto.expectedProfit());
            long fees = dto.fees() == null ? 0L : Math.max(0L, dto.fees());
            if (fees > 0L && expectedProfit > fees * 100L) {
                score -= 10D;
            }
        }

        return clamp(score, 0D, 100D);
    }

    private boolean shouldApplyPartialPenalty(UnifiedFlipDto dto) {
        if (dto == null || !dto.partial()) {
            return false;
        }
        if (!flippingModelProperties.isElectionPenaltySoftened()) {
            return true;
        }
        List<String> partialReasons = dto.partialReasons();
        if (partialReasons == null || partialReasons.isEmpty()) {
            return true;
        }
        for (String reason : partialReasons) {
            if (reason == null || reason.isBlank()) {
                continue;
            }
            if (!MISSING_ELECTION_DATA_REASON.equals(reason)) {
                return true;
            }
        }
        return false;
    }

    private double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    private record GoodnessCacheKey(
            Long snapshotEpochMillis,
            FlipType flipType,
            boolean scoringV2Enabled,
            boolean recommendationGatesEnabled,
            boolean outlierProtectionEnabled,
            boolean electionPenaltySoftened,
            double minRecommendationLiquidityScore,
            long minRecommendationExpectedProfit,
            double minConfidenceScore
    ) {
        private static GoodnessCacheKey from(Long snapshotEpochMillis,
                                             FlipType flipType,
                                             FlippingModelProperties properties) {
            return new GoodnessCacheKey(
                    snapshotEpochMillis,
                    flipType,
                    properties.isScoringV2Enabled(),
                    properties.isRecommendationGatesEnabled(),
                    properties.isOutlierProtectionEnabled(),
                    properties.isElectionPenaltySoftened(),
                    properties.getMinRecommendationLiquidityScore(),
                    properties.getMinRecommendationExpectedProfit(),
                    properties.getMinConfidenceScore()
            );
        }
    }

    private record CachedGoodnessRanking(
            List<FlipGoodnessDto> ranking,
            long createdAtEpochMillis
    ) {
    }
}
