package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.flippingstorage.FlipCurrentEntity;
import com.skyblockflipper.backend.model.flippingstorage.FlipDefinitionEntity;
import com.skyblockflipper.backend.repository.FlipCurrentRepository;
import com.skyblockflipper.backend.repository.FlipDefinitionRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class UnifiedFlipCurrentReadService {

    private final FlipCurrentRepository flipCurrentRepository;
    private final FlipDefinitionRepository flipDefinitionRepository;
    private final StoredFlipDtoMapper storedFlipDtoMapper;

    public UnifiedFlipCurrentReadService(FlipCurrentRepository flipCurrentRepository,
                                         FlipDefinitionRepository flipDefinitionRepository,
                                         StoredFlipDtoMapper storedFlipDtoMapper) {
        this.flipCurrentRepository = flipCurrentRepository;
        this.flipDefinitionRepository = flipDefinitionRepository;
        this.storedFlipDtoMapper = storedFlipDtoMapper;
    }

    @Transactional(readOnly = true)
    public List<UnifiedFlipDto> listCurrent(FlipType flipType) {
        List<FlipCurrentEntity> currentEntities = flipType == null
                ? flipCurrentRepository.findAll()
                : flipCurrentRepository.findAllByFlipType(flipType);
        if (currentEntities.isEmpty()) {
            return List.of();
        }
        Map<String, FlipDefinitionEntity> definitionsByKey = definitionsByKey(currentEntities);
        List<UnifiedFlipDto> dtos = new ArrayList<>(currentEntities.size());
        for (FlipCurrentEntity current : currentEntities) {
            FlipDefinitionEntity definition = definitionsByKey.get(current.getFlipKey());
            UnifiedFlipDto dto = storedFlipDtoMapper.toDto(current, definition);
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
        Page<FlipCurrentEntity> page = flipType == null
                ? flipCurrentRepository.findAll(pageable)
                : flipCurrentRepository.findAllByFlipType(flipType, pageable);
        if (page.isEmpty()) {
            return new PageImpl<>(List.of(), pageable, page.getTotalElements());
        }
        Map<String, FlipDefinitionEntity> definitionsByKey = definitionsByKey(page.getContent());
        List<UnifiedFlipDto> content = page.getContent().stream()
                .map(current -> storedFlipDtoMapper.toDto(current, definitionsByKey.get(current.getFlipKey())))
                .filter(dto -> dto != null)
                .toList();
        return new PageImpl<>(content, pageable, page.getTotalElements());
    }

    @Transactional(readOnly = true)
    public Optional<UnifiedFlipDto> findByStableFlipId(UUID stableFlipId) {
        Optional<FlipCurrentEntity> current = flipCurrentRepository.findByStableFlipId(stableFlipId);
        if (current.isEmpty()) {
            return Optional.empty();
        }
        Optional<FlipDefinitionEntity> definition = flipDefinitionRepository.findById(current.get().getFlipKey());
        return definition.map(flipDefinitionEntity -> storedFlipDtoMapper.toDto(current.get(), flipDefinitionEntity));
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

    private Map<String, FlipDefinitionEntity> definitionsByKey(List<FlipCurrentEntity> currentEntities) {
        List<String> keys = currentEntities.stream().map(FlipCurrentEntity::getFlipKey).distinct().toList();
        Map<String, FlipDefinitionEntity> result = new LinkedHashMap<>();
        for (FlipDefinitionEntity definition : flipDefinitionRepository.findAllById(keys)) {
            result.put(definition.getFlipKey(), definition);
        }
        return result;
    }
}
