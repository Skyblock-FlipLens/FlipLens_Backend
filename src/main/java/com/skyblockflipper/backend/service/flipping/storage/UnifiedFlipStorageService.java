package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.flippingstorage.FlipCurrentEntity;
import com.skyblockflipper.backend.model.flippingstorage.FlipDefinitionEntity;
import com.skyblockflipper.backend.model.flippingstorage.FlipTrendSegmentEntity;
import com.skyblockflipper.backend.repository.FlipCurrentRepository;
import com.skyblockflipper.backend.repository.FlipDefinitionRepository;
import com.skyblockflipper.backend.repository.FlipTrendSegmentRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContext;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class UnifiedFlipStorageService {

    private final FlipDefinitionRepository flipDefinitionRepository;
    private final FlipCurrentRepository flipCurrentRepository;
    private final FlipTrendSegmentRepository flipTrendSegmentRepository;
    private final FlipIdentityService flipIdentityService;
    private final UnifiedFlipDtoMapper unifiedFlipDtoMapper;
    private final FlipCalculationContextService flipCalculationContextService;
    private final FlipStorageProperties flipStorageProperties;
    private final ObjectMapper objectMapper;

    public UnifiedFlipStorageService(FlipDefinitionRepository flipDefinitionRepository,
                                     FlipCurrentRepository flipCurrentRepository,
                                     FlipTrendSegmentRepository flipTrendSegmentRepository,
                                     FlipIdentityService flipIdentityService,
                                     UnifiedFlipDtoMapper unifiedFlipDtoMapper,
                                     FlipCalculationContextService flipCalculationContextService,
                                     FlipStorageProperties flipStorageProperties,
                                     ObjectMapper objectMapper) {
        this.flipDefinitionRepository = flipDefinitionRepository;
        this.flipCurrentRepository = flipCurrentRepository;
        this.flipTrendSegmentRepository = flipTrendSegmentRepository;
        this.flipIdentityService = flipIdentityService;
        this.unifiedFlipDtoMapper = unifiedFlipDtoMapper;
        this.flipCalculationContextService = flipCalculationContextService;
        this.flipStorageProperties = flipStorageProperties;
        this.objectMapper = objectMapper;
    }

    public boolean existsForSnapshot(long snapshotEpochMillis) {
        return flipCurrentRepository.existsBySnapshotTimestampEpochMillis(snapshotEpochMillis);
    }

    @Transactional
    public void persistSnapshotFlips(List<Flip> flips, Instant snapshotTimestamp) {
        persistSnapshotFlipsInternal(flips, snapshotTimestamp, false);
    }

    @Transactional
    public void persistSnapshotFlipsForced(List<Flip> flips, Instant snapshotTimestamp) {
        persistSnapshotFlipsInternal(flips, snapshotTimestamp, true);
    }

    private void persistSnapshotFlipsInternal(List<Flip> flips, Instant snapshotTimestamp, boolean forceWrite) {
        if (!forceWrite && !flipStorageProperties.isDualWriteEnabled()) {
            return;
        }
        if (flips == null || flips.isEmpty() || snapshotTimestamp == null) {
            return;
        }

        long snapshotEpochMillis = snapshotTimestamp.toEpochMilli();
        FlipCalculationContext context = flipCalculationContextService.loadContextAsOf(snapshotTimestamp);

        List<ComputedFlip> computedFlips = new ArrayList<>(flips.size());
        for (Flip flip : flips) {
            if (flip == null) {
                continue;
            }
            FlipIdentityService.Identity identity = flipIdentityService.derive(flip);
            UnifiedFlipDto dto = unifiedFlipDtoMapper.toDto(flip, context);
            if (dto == null) {
                continue;
            }
            computedFlips.add(new ComputedFlip(identity, dto));
        }
        if (computedFlips.isEmpty()) {
            return;
        }

        List<String> flipKeys = computedFlips.stream().map(entry -> entry.identity().flipKey()).toList();
        Map<String, FlipDefinitionEntity> definitionsByKey = toMap(flipDefinitionRepository.findAllById(flipKeys));
        Map<String, FlipCurrentEntity> currentByKey = toMapCurrent(flipCurrentRepository.findAllById(flipKeys));
        Map<String, FlipTrendSegmentEntity> latestSegmentsByKey = latestSegmentsByFlipKey(flipKeys);

        long now = System.currentTimeMillis();
        List<FlipDefinitionEntity> definitionsToSave = new ArrayList<>(computedFlips.size());
        List<FlipCurrentEntity> currentToSave = new ArrayList<>(computedFlips.size());
        List<FlipTrendSegmentEntity> segmentsToSave = new ArrayList<>();

        for (ComputedFlip computedFlip : computedFlips) {
            FlipIdentityService.Identity identity = computedFlip.identity();
            UnifiedFlipDto dto = computedFlip.dto();
            String flipKey = identity.flipKey();

            FlipDefinitionEntity definition = definitionsByKey.get(flipKey);
            if (definition == null) {
                definition = new FlipDefinitionEntity();
                definition.setFlipKey(flipKey);
                definition.setCreatedAtEpochMillis(now);
            }
            definition.setStableFlipId(identity.stableFlipId());
            definition.setFlipType(identity.flipType());
            definition.setResultItemId(identity.resultItemId());
            definition.setStepsJson(identity.stepsJson());
            definition.setConstraintsJson(identity.constraintsJson());
            definition.setKeyVersion(identity.keyVersion());
            definition.setUpdatedAtEpochMillis(now);
            definitionsToSave.add(definition);

            FlipCurrentEntity current = currentByKey.get(flipKey);
            if (current == null) {
                current = new FlipCurrentEntity();
                current.setFlipKey(flipKey);
            }
            current.setStableFlipId(identity.stableFlipId());
            current.setFlipType(identity.flipType());
            current.setSnapshotTimestampEpochMillis(snapshotEpochMillis);
            current.setRequiredCapital(dto.requiredCapital());
            current.setExpectedProfit(dto.expectedProfit());
            current.setRoi(dto.roi());
            current.setRoiPerHour(dto.roiPerHour());
            current.setDurationSeconds(dto.durationSeconds());
            current.setFees(dto.fees());
            current.setLiquidityScore(dto.liquidityScore());
            current.setRiskScore(dto.riskScore());
            current.setPartial(dto.partial());
            current.setPartialReasonsJson(writeJson(dto.partialReasons()));
            current.setUpdatedAtEpochMillis(now);
            currentToSave.add(current);

            FlipTrendSegmentEntity latestSegment = latestSegmentsByKey.get(flipKey);
            if (latestSegment != null && shouldExtendSegment(latestSegment, dto)) {
                latestSegment.setValidToSnapshotEpochMillis(snapshotEpochMillis);
                latestSegment.setSampleCount(Math.max(1, latestSegment.getSampleCount()) + 1);
                latestSegment.setUpdatedAtEpochMillis(now);
                segmentsToSave.add(latestSegment);
            } else {
                FlipTrendSegmentEntity newSegment = new FlipTrendSegmentEntity();
                newSegment.setFlipKey(flipKey);
                newSegment.setValidFromSnapshotEpochMillis(snapshotEpochMillis);
                newSegment.setValidToSnapshotEpochMillis(snapshotEpochMillis);
                newSegment.setRequiredCapital(dto.requiredCapital());
                newSegment.setExpectedProfit(dto.expectedProfit());
                newSegment.setRoi(dto.roi());
                newSegment.setRoiPerHour(dto.roiPerHour());
                newSegment.setDurationSeconds(dto.durationSeconds());
                newSegment.setFees(dto.fees());
                newSegment.setLiquidityScore(dto.liquidityScore());
                newSegment.setRiskScore(dto.riskScore());
                newSegment.setPartial(dto.partial());
                newSegment.setSampleCount(1);
                newSegment.setCreatedAtEpochMillis(now);
                newSegment.setUpdatedAtEpochMillis(now);
                segmentsToSave.add(newSegment);
            }
        }

        flipDefinitionRepository.saveAll(definitionsToSave);
        flipCurrentRepository.saveAll(currentToSave);
        flipTrendSegmentRepository.saveAll(segmentsToSave);
    }

    private Map<String, FlipDefinitionEntity> toMap(List<FlipDefinitionEntity> entities) {
        Map<String, FlipDefinitionEntity> byKey = new LinkedHashMap<>();
        for (FlipDefinitionEntity entity : entities) {
            byKey.put(entity.getFlipKey(), entity);
        }
        return byKey;
    }

    private Map<String, FlipCurrentEntity> toMapCurrent(List<FlipCurrentEntity> entities) {
        Map<String, FlipCurrentEntity> byKey = new LinkedHashMap<>();
        for (FlipCurrentEntity entity : entities) {
            byKey.put(entity.getFlipKey(), entity);
        }
        return byKey;
    }

    private Map<String, FlipTrendSegmentEntity> latestSegmentsByFlipKey(Collection<String> flipKeys) {
        if (flipKeys.isEmpty()) {
            return Map.of();
        }
        List<FlipTrendSegmentEntity> sorted = flipTrendSegmentRepository
                .findByFlipKeyInOrderByFlipKeyAscValidToSnapshotEpochMillisDesc(flipKeys);
        Map<String, FlipTrendSegmentEntity> latest = new HashMap<>();
        for (FlipTrendSegmentEntity segment : sorted) {
            latest.putIfAbsent(segment.getFlipKey(), segment);
        }
        return latest;
    }

    private boolean shouldExtendSegment(FlipTrendSegmentEntity segment, UnifiedFlipDto dto) {
        if (segment.isPartial() != dto.partial()) {
            return false;
        }

        double relativeThreshold = Math.max(0.0D, flipStorageProperties.getTrendRelativeThreshold());
        double scoreDeltaThreshold = Math.max(0.0D, flipStorageProperties.getTrendScoreDeltaThreshold());

        boolean profitStable = relativeChangeWithin(segment.getExpectedProfit(), dto.expectedProfit(), relativeThreshold);
        boolean roiPerHourStable = relativeChangeWithin(segment.getRoiPerHour(), dto.roiPerHour(), relativeThreshold);
        boolean liquidityStable = absoluteChangeWithin(segment.getLiquidityScore(), dto.liquidityScore(), scoreDeltaThreshold);
        boolean riskStable = absoluteChangeWithin(segment.getRiskScore(), dto.riskScore(), scoreDeltaThreshold);

        return profitStable && roiPerHourStable && liquidityStable && riskStable;
    }

    private boolean relativeChangeWithin(Long oldValue, Long newValue, double threshold) {
        return relativeChangeWithin(
                oldValue == null ? null : oldValue.doubleValue(),
                newValue == null ? null : newValue.doubleValue(),
                threshold
        );
    }

    private boolean relativeChangeWithin(Double oldValue, Double newValue, double threshold) {
        if (oldValue == null && newValue == null) {
            return true;
        }
        if (oldValue == null || newValue == null) {
            return false;
        }
        double baseline = Math.max(Math.abs(oldValue), 1e-9D);
        double change = Math.abs(newValue - oldValue) / baseline;
        return change <= threshold;
    }

    private boolean absoluteChangeWithin(Double oldValue, Double newValue, double threshold) {
        if (oldValue == null && newValue == null) {
            return true;
        }
        if (oldValue == null || newValue == null) {
            return false;
        }
        return Math.abs(newValue - oldValue) <= threshold;
    }

    private String writeJson(List<String> values) {
        try {
            return objectMapper.writeValueAsString(values == null ? List.of() : values);
        } catch (Exception e) {
            return "[]";
        }
    }

    private record ComputedFlip(
            FlipIdentityService.Identity identity,
            UnifiedFlipDto dto
    ) {
    }
}
