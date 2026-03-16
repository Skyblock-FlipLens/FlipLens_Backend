package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.dto.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.flippingstorage.FlipCurrentEntity;
import com.skyblockflipper.backend.repository.FlipCurrentRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
public class UnifiedFlipCurrentReadService {

    private final FlipCurrentRepository flipCurrentRepository;
    private final StoredFlipDtoMapper storedFlipDtoMapper;

    public UnifiedFlipCurrentReadService(FlipCurrentRepository flipCurrentRepository,
                                         StoredFlipDtoMapper storedFlipDtoMapper) {
        this.flipCurrentRepository = flipCurrentRepository;
        this.storedFlipDtoMapper = storedFlipDtoMapper;
    }

    @Transactional(readOnly = true)
    public List<UnifiedFlipDto> listCurrent(FlipType flipType) {
        List<FlipCurrentRepository.CurrentDefinitionProjection> rows = flipType == null
                ? flipCurrentRepository.findAllWithDefinition()
                : flipCurrentRepository.findAllWithDefinitionByFlipType(flipType);
        if (rows.isEmpty()) {
            return List.of();
        }
        List<UnifiedFlipDto> dtos = new ArrayList<>(rows.size());
        for (FlipCurrentRepository.CurrentDefinitionProjection row : rows) {
            UnifiedFlipDto dto = storedFlipDtoMapper.toDto(row.getCurrent(), row.getDefinition());
            if (dto != null) {
                dtos.add(dto);
            }
        }
        return List.copyOf(dtos);
    }

    @Transactional(readOnly = true)
    public Page<UnifiedFlipDto> listCurrentPage(FlipType flipType, Pageable pageable) {
        if (pageable == null || pageable.isUnpaged()) {
            List<UnifiedFlipDto> values = listCurrent(flipType);
            return new PageImpl<>(values);
        }
        Page<FlipCurrentRepository.CurrentDefinitionProjection> page = flipType == null
                ? flipCurrentRepository.findAllWithDefinition(pageable)
                : flipCurrentRepository.findAllWithDefinitionByFlipType(flipType, pageable);
        if (page.isEmpty()) {
            return new PageImpl<>(List.of(), pageable, page.getTotalElements());
        }
        List<UnifiedFlipDto> content = new ArrayList<>(page.getNumberOfElements());
        for (FlipCurrentRepository.CurrentDefinitionProjection row : page.getContent()) {
            UnifiedFlipDto dto = storedFlipDtoMapper.toDto(row.getCurrent(), row.getDefinition());
            if (dto == null) {
                throw new IllegalStateException("StoredFlipDtoMapper returned null for paged current projection");
            }
            content.add(dto);
        }
        return new PageImpl<>(List.copyOf(content), pageable, page.getTotalElements());
    }

    @Transactional(readOnly = true)
    public Page<UnifiedFlipDto> listCurrentFilteredPage(FlipType flipType,
                                                        Double minLiquidityScore,
                                                        Double maxRiskScore,
                                                        Long minExpectedProfit,
                                                        Double minRoi,
                                                        Double minRoiPerHour,
                                                        Long maxRequiredCapital,
                                                        Boolean partial,
                                                        Pageable pageable) {
        Pageable effectivePageable = pageable == null ? Pageable.unpaged() : pageable;
        Page<FlipCurrentRepository.CurrentDefinitionProjection> page = flipCurrentRepository.findFilteredWithDefinition(
                flipType,
                minLiquidityScore,
                maxRiskScore,
                minExpectedProfit,
                minRoi,
                minRoiPerHour,
                maxRequiredCapital,
                partial,
                effectivePageable
        );
        if (page.isEmpty()) {
            return new PageImpl<>(List.of(), effectivePageable, page.getTotalElements());
        }

        List<UnifiedFlipDto> content = new ArrayList<>(page.getNumberOfElements());
        for (FlipCurrentRepository.CurrentDefinitionProjection row : page.getContent()) {
            UnifiedFlipDto dto = storedFlipDtoMapper.toDto(row.getCurrent(), row.getDefinition());
            if (dto == null) {
                throw new IllegalStateException("StoredFlipDtoMapper returned null for filtered current projection");
            }
            content.add(dto);
        }
        return new PageImpl<>(List.copyOf(content), effectivePageable, page.getTotalElements());
    }

    @Transactional(readOnly = true)
    public Optional<UnifiedFlipDto> findByStableFlipId(UUID stableFlipId) {
        return flipCurrentRepository.findByStableFlipIdWithDefinition(stableFlipId)
                .map(row -> storedFlipDtoMapper.toDto(row.getCurrent(), row.getDefinition()));
    }

    @Transactional(readOnly = true)
    public Optional<Long> latestSnapshotEpochMillis() {
        return flipCurrentRepository.findMaxSnapshotTimestampEpochMillis();
    }

    @Transactional(readOnly = true)
    public List<UnifiedFlipDto> listCurrentScoringDtos(FlipType flipType) {
        List<FlipCurrentEntity> rows = flipType == null
                ? flipCurrentRepository.findAll()
                : flipCurrentRepository.findAllByFlipType(flipType);
        if (rows.isEmpty()) {
            return List.of();
        }
        List<UnifiedFlipDto> result = new ArrayList<>(rows.size());
        for (FlipCurrentEntity row : rows) {
            result.add(new UnifiedFlipDto(
                    row.getStableFlipId(),
                    row.getFlipType(),
                    List.of(),
                    List.of(),
                    row.getRequiredCapital(),
                    row.getExpectedProfit(),
                    row.getRoi(),
                    row.getRoiPerHour(),
                    row.getDurationSeconds(),
                    row.getFees(),
                    row.getLiquidityScore(),
                    row.getRiskScore(),
                    Instant.ofEpochMilli(row.getSnapshotTimestampEpochMillis()),
                    row.isPartial(),
                    storedFlipDtoMapper.parsePartialReasonsJson(row.getPartialReasonsJson()),
                    List.of(),
                    List.of()
            ));
        }
        return List.copyOf(result);
    }

    @Transactional(readOnly = true)
    public List<UnifiedFlipDto> listCurrentByStableFlipIds(List<UUID> stableFlipIds) {
        if (stableFlipIds == null || stableFlipIds.isEmpty()) {
            return List.of();
        }
        List<UUID> uniqueIds = stableFlipIds.stream()
                .filter(Objects::nonNull)
                .collect(java.util.stream.Collectors.collectingAndThen(
                        java.util.stream.Collectors.toCollection(LinkedHashSet::new),
                        ArrayList::new
                ));
        if (uniqueIds.isEmpty()) {
            return List.of();
        }
        Map<UUID, UnifiedFlipDto> byStableId = new LinkedHashMap<>();
        for (FlipCurrentRepository.CurrentDefinitionProjection row
                : flipCurrentRepository.findAllWithDefinitionByStableFlipIds(uniqueIds)) {
            UnifiedFlipDto dto = storedFlipDtoMapper.toDto(row.getCurrent(), row.getDefinition());
            if (dto != null && dto.id() != null) {
                byStableId.put(dto.id(), dto);
            }
        }
        if (byStableId.isEmpty()) {
            return List.of();
        }
        List<UnifiedFlipDto> ordered = new ArrayList<>(uniqueIds.size());
        for (UUID stableId : uniqueIds) {
            UnifiedFlipDto dto = byStableId.get(stableId);
            if (dto != null) {
                ordered.add(dto);
            }
        }
        return List.copyOf(ordered);
    }

    @Transactional(readOnly = true)
    public Map<FlipType, Long> countsByType() {
        EnumMap<FlipType, Long> counts = new EnumMap<>(FlipType.class);
        for (FlipType flipType : FlipType.values()) {
            counts.put(flipType, 0L);
        }
        for (FlipCurrentRepository.FlipTypeCountProjection projection : flipCurrentRepository.countByFlipType()) {
            if (projection.getFlipType() == null) {
                continue;
            }
            counts.put(projection.getFlipType(), projection.getCount());
        }
        return counts;
    }

    @Transactional(readOnly = true)
    public Optional<CurrentSummary> currentSummary() {
        FlipCurrentRepository.CurrentSummaryProjection projection = flipCurrentRepository.summarizeCurrent();
        if (projection == null || projection.getLatestSnapshotEpochMillis() == null) {
            return Optional.empty();
        }
        return Optional.of(new CurrentSummary(
                projection.getTotalCount(),
                projection.getMaxExpectedProfit(),
                projection.getLatestSnapshotEpochMillis()
        ));
    }

    public record CurrentSummary(long totalCount, Long maxExpectedProfit, long latestSnapshotEpochMillis) {
    }

}
