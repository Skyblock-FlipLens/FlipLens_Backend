package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.repository.FlipCurrentRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.EnumMap;
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
        List<UnifiedFlipDto> content = page.getContent().stream()
                .map(row -> storedFlipDtoMapper.toDto(row.getCurrent(), row.getDefinition()))
                .filter(Objects::nonNull)
                .toList();
        return new PageImpl<>(content, pageable, page.getTotalElements());
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

}
