package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
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
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UnifiedFlipStorageServiceTest {

    @Test
    void clearSnapshotDataUsesBulkValidityWindowDelete() {
        FlipDefinitionRepository flipDefinitionRepository = mock(FlipDefinitionRepository.class);
        FlipCurrentRepository flipCurrentRepository = mock(FlipCurrentRepository.class);
        FlipTrendSegmentRepository flipTrendSegmentRepository = mock(FlipTrendSegmentRepository.class);
        FlipIdentityService flipIdentityService = mock(FlipIdentityService.class);
        UnifiedFlipDtoMapper unifiedFlipDtoMapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipStorageProperties properties = new FlipStorageProperties();
        UnifiedFlipStorageService service = new UnifiedFlipStorageService(
                flipDefinitionRepository,
                flipCurrentRepository,
                flipTrendSegmentRepository,
                flipIdentityService,
                unifiedFlipDtoMapper,
                contextService,
                properties,
                new ObjectMapper()
        );
        long snapshotEpochMillis = Instant.parse("2026-02-20T12:00:00Z").toEpochMilli();

        service.clearSnapshotData(snapshotEpochMillis);

        verify(flipCurrentRepository).deleteBySnapshotTimestampEpochMillis(snapshotEpochMillis);
        verify(flipTrendSegmentRepository).deleteByValidityWindow(snapshotEpochMillis, snapshotEpochMillis);
    }

    @Test
    void persistSnapshotFlipsSkipsWhenCurrentSnapshotIsNewerOrEqual() {
        FlipDefinitionRepository flipDefinitionRepository = mock(FlipDefinitionRepository.class);
        FlipCurrentRepository flipCurrentRepository = mock(FlipCurrentRepository.class);
        FlipTrendSegmentRepository flipTrendSegmentRepository = mock(FlipTrendSegmentRepository.class);
        FlipIdentityService flipIdentityService = mock(FlipIdentityService.class);
        UnifiedFlipDtoMapper unifiedFlipDtoMapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipStorageProperties properties = new FlipStorageProperties();
        properties.setDualWriteEnabled(true);
        UnifiedFlipStorageService service = new UnifiedFlipStorageService(
                flipDefinitionRepository,
                flipCurrentRepository,
                flipTrendSegmentRepository,
                flipIdentityService,
                unifiedFlipDtoMapper,
                contextService,
                properties,
                new ObjectMapper()
        );
        Instant snapshotTimestamp = Instant.parse("2026-02-20T12:00:00Z");
        long snapshotEpochMillis = snapshotTimestamp.toEpochMilli();
        Flip flip = mock(Flip.class);
        when(contextService.loadContextAsOf(snapshotTimestamp)).thenReturn(FlipCalculationContext.standard(null));
        when(flipIdentityService.derive(flip)).thenReturn(new FlipIdentityService.Identity(
                "key-1",
                UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                FlipType.BAZAAR,
                "ENCHANTED_SUGAR",
                "[]",
                "[]",
                1
        ));
        when(unifiedFlipDtoMapper.toDto(any(Flip.class), any(FlipCalculationContext.class)))
                .thenReturn(sampleDto(snapshotTimestamp));
        FlipCurrentEntity existingCurrent = new FlipCurrentEntity();
        existingCurrent.setFlipKey("key-1");
        existingCurrent.setSnapshotTimestampEpochMillis(snapshotEpochMillis + 1L);
        when(flipDefinitionRepository.findAllById(List.of("key-1"))).thenReturn(List.of());
        when(flipCurrentRepository.findAllById(List.of("key-1"))).thenReturn(List.of(existingCurrent));
        when(flipTrendSegmentRepository.findByFlipKeyInOrderByFlipKeyAscValidToSnapshotEpochMillisDesc(List.of("key-1")))
                .thenReturn(List.of());

        service.persistSnapshotFlips(List.of(flip), snapshotTimestamp);

        verify(flipDefinitionRepository).saveAll(List.of());
        verify(flipCurrentRepository).saveAll(List.of());
        verify(flipTrendSegmentRepository).saveAll(List.of());
        assertEquals(snapshotEpochMillis + 1L, existingCurrent.getSnapshotTimestampEpochMillis());
    }

    @Test
    void persistSnapshotFlipsSkipsWhenLatestTrendSegmentIsNewerOrEqual() {
        FlipDefinitionRepository flipDefinitionRepository = mock(FlipDefinitionRepository.class);
        FlipCurrentRepository flipCurrentRepository = mock(FlipCurrentRepository.class);
        FlipTrendSegmentRepository flipTrendSegmentRepository = mock(FlipTrendSegmentRepository.class);
        FlipIdentityService flipIdentityService = mock(FlipIdentityService.class);
        UnifiedFlipDtoMapper unifiedFlipDtoMapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipStorageProperties properties = new FlipStorageProperties();
        properties.setDualWriteEnabled(true);
        UnifiedFlipStorageService service = new UnifiedFlipStorageService(
                flipDefinitionRepository,
                flipCurrentRepository,
                flipTrendSegmentRepository,
                flipIdentityService,
                unifiedFlipDtoMapper,
                contextService,
                properties,
                new ObjectMapper()
        );
        Instant snapshotTimestamp = Instant.parse("2026-02-20T12:00:00Z");
        long snapshotEpochMillis = snapshotTimestamp.toEpochMilli();
        Flip flip = mock(Flip.class);
        when(contextService.loadContextAsOf(snapshotTimestamp)).thenReturn(FlipCalculationContext.standard(null));
        when(flipIdentityService.derive(flip)).thenReturn(new FlipIdentityService.Identity(
                "key-2",
                UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
                FlipType.BAZAAR,
                "ENCHANTED_SUGAR",
                "[]",
                "[]",
                1
        ));
        when(unifiedFlipDtoMapper.toDto(any(Flip.class), any(FlipCalculationContext.class)))
                .thenReturn(sampleDto(snapshotTimestamp));
        FlipCurrentEntity existingCurrent = new FlipCurrentEntity();
        existingCurrent.setFlipKey("key-2");
        existingCurrent.setSnapshotTimestampEpochMillis(snapshotEpochMillis - 10L);
        FlipTrendSegmentEntity latestSegment = new FlipTrendSegmentEntity();
        latestSegment.setFlipKey("key-2");
        latestSegment.setValidFromSnapshotEpochMillis(snapshotEpochMillis - 5L);
        latestSegment.setValidToSnapshotEpochMillis(snapshotEpochMillis);
        latestSegment.setSampleCount(3);
        when(flipDefinitionRepository.findAllById(List.of("key-2"))).thenReturn(List.of());
        when(flipCurrentRepository.findAllById(List.of("key-2"))).thenReturn(List.of(existingCurrent));
        when(flipTrendSegmentRepository.findByFlipKeyInOrderByFlipKeyAscValidToSnapshotEpochMillisDesc(List.of("key-2")))
                .thenReturn(List.of(latestSegment));

        service.persistSnapshotFlips(List.of(flip), snapshotTimestamp);

        verify(flipDefinitionRepository).saveAll(List.of());
        verify(flipCurrentRepository).saveAll(List.of());
        verify(flipTrendSegmentRepository).saveAll(List.of());
        assertEquals(snapshotEpochMillis, latestSegment.getValidToSnapshotEpochMillis());
        assertEquals(3, latestSegment.getSampleCount());
    }

    @Test
    void persistSnapshotFlipsCreatesDefinitionCurrentAndSegmentForNewFlip() {
        FlipDefinitionRepository flipDefinitionRepository = mock(FlipDefinitionRepository.class);
        FlipCurrentRepository flipCurrentRepository = mock(FlipCurrentRepository.class);
        FlipTrendSegmentRepository flipTrendSegmentRepository = mock(FlipTrendSegmentRepository.class);
        FlipIdentityService flipIdentityService = mock(FlipIdentityService.class);
        UnifiedFlipDtoMapper unifiedFlipDtoMapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipStorageProperties properties = new FlipStorageProperties();
        properties.setDualWriteEnabled(true);
        UnifiedFlipStorageService service = new UnifiedFlipStorageService(
                flipDefinitionRepository,
                flipCurrentRepository,
                flipTrendSegmentRepository,
                flipIdentityService,
                unifiedFlipDtoMapper,
                contextService,
                properties,
                new ObjectMapper()
        );
        Instant snapshotTimestamp = Instant.parse("2026-02-21T12:00:00Z");
        long snapshotEpochMillis = snapshotTimestamp.toEpochMilli();
        Flip flip = mock(Flip.class);
        when(contextService.loadContextAsOf(snapshotTimestamp)).thenReturn(FlipCalculationContext.standard(null));
        when(flipIdentityService.derive(flip)).thenReturn(new FlipIdentityService.Identity(
                "key-new",
                UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc"),
                FlipType.BAZAAR,
                "ENCHANTED_SUGAR",
                "[{\"type\":\"BUY\"}]",
                "[{\"type\":\"MIN_CAPITAL\"}]",
                1
        ));
        when(unifiedFlipDtoMapper.toDto(any(Flip.class), any(FlipCalculationContext.class)))
                .thenReturn(sampleDto(snapshotTimestamp));
        when(flipDefinitionRepository.findAllById(List.of("key-new"))).thenReturn(List.of());
        when(flipCurrentRepository.findAllById(List.of("key-new"))).thenReturn(List.of());
        when(flipTrendSegmentRepository.findByFlipKeyInOrderByFlipKeyAscValidToSnapshotEpochMillisDesc(List.of("key-new")))
                .thenReturn(List.of());

        service.persistSnapshotFlips(List.of(flip), snapshotTimestamp);

        ArgumentCaptor<List<FlipDefinitionEntity>> definitionsCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<FlipCurrentEntity>> currentCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<FlipTrendSegmentEntity>> segmentCaptor = ArgumentCaptor.forClass(List.class);
        verify(flipDefinitionRepository).saveAll(definitionsCaptor.capture());
        verify(flipCurrentRepository).saveAll(currentCaptor.capture());
        verify(flipTrendSegmentRepository).saveAll(segmentCaptor.capture());
        assertEquals(1, definitionsCaptor.getValue().size());
        assertEquals("key-new", definitionsCaptor.getValue().getFirst().getFlipKey());
        assertEquals(1, currentCaptor.getValue().size());
        assertEquals(snapshotEpochMillis, currentCaptor.getValue().getFirst().getSnapshotTimestampEpochMillis());
        assertEquals(1, segmentCaptor.getValue().size());
        assertEquals(snapshotEpochMillis, segmentCaptor.getValue().getFirst().getValidFromSnapshotEpochMillis());
        assertEquals(snapshotEpochMillis, segmentCaptor.getValue().getFirst().getValidToSnapshotEpochMillis());
        assertEquals(1, segmentCaptor.getValue().getFirst().getSampleCount());
    }

    @Test
    void persistSnapshotFlipsExtendsExistingStableSegment() {
        FlipDefinitionRepository flipDefinitionRepository = mock(FlipDefinitionRepository.class);
        FlipCurrentRepository flipCurrentRepository = mock(FlipCurrentRepository.class);
        FlipTrendSegmentRepository flipTrendSegmentRepository = mock(FlipTrendSegmentRepository.class);
        FlipIdentityService flipIdentityService = mock(FlipIdentityService.class);
        UnifiedFlipDtoMapper unifiedFlipDtoMapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipStorageProperties properties = new FlipStorageProperties();
        properties.setDualWriteEnabled(true);
        properties.setTrendRelativeThreshold(0.10D);
        properties.setTrendScoreDeltaThreshold(5.0D);
        UnifiedFlipStorageService service = new UnifiedFlipStorageService(
                flipDefinitionRepository,
                flipCurrentRepository,
                flipTrendSegmentRepository,
                flipIdentityService,
                unifiedFlipDtoMapper,
                contextService,
                properties,
                new ObjectMapper()
        );
        Instant snapshotTimestamp = Instant.parse("2026-02-21T12:00:00Z");
        long snapshotEpochMillis = snapshotTimestamp.toEpochMilli();
        Flip flip = mock(Flip.class);
        when(contextService.loadContextAsOf(snapshotTimestamp)).thenReturn(FlipCalculationContext.standard(null));
        when(flipIdentityService.derive(flip)).thenReturn(new FlipIdentityService.Identity(
                "key-stable",
                UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd"),
                FlipType.BAZAAR,
                "ENCHANTED_SUGAR",
                "[]",
                "[]",
                1
        ));
        when(unifiedFlipDtoMapper.toDto(any(Flip.class), any(FlipCalculationContext.class)))
                .thenReturn(sampleDto(snapshotTimestamp));
        FlipDefinitionEntity existingDefinition = new FlipDefinitionEntity();
        existingDefinition.setFlipKey("key-stable");
        existingDefinition.setStableFlipId(UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd"));
        FlipCurrentEntity existingCurrent = new FlipCurrentEntity();
        existingCurrent.setFlipKey("key-stable");
        existingCurrent.setSnapshotTimestampEpochMillis(snapshotEpochMillis - 100L);
        FlipTrendSegmentEntity latestSegment = new FlipTrendSegmentEntity();
        latestSegment.setFlipKey("key-stable");
        latestSegment.setValidFromSnapshotEpochMillis(snapshotEpochMillis - 200L);
        latestSegment.setValidToSnapshotEpochMillis(snapshotEpochMillis - 1L);
        latestSegment.setExpectedProfit(100_000L);
        latestSegment.setRoiPerHour(0.8D);
        latestSegment.setLiquidityScore(80D);
        latestSegment.setRiskScore(10D);
        latestSegment.setPartial(false);
        latestSegment.setSampleCount(2);
        when(flipDefinitionRepository.findAllById(List.of("key-stable"))).thenReturn(List.of(existingDefinition));
        when(flipCurrentRepository.findAllById(List.of("key-stable"))).thenReturn(List.of(existingCurrent));
        when(flipTrendSegmentRepository.findByFlipKeyInOrderByFlipKeyAscValidToSnapshotEpochMillisDesc(List.of("key-stable")))
                .thenReturn(List.of(latestSegment));

        service.persistSnapshotFlips(List.of(flip), snapshotTimestamp);

        ArgumentCaptor<List<FlipTrendSegmentEntity>> segmentCaptor = ArgumentCaptor.forClass(List.class);
        verify(flipTrendSegmentRepository).saveAll(segmentCaptor.capture());
        assertEquals(1, segmentCaptor.getValue().size());
        assertEquals(snapshotEpochMillis, latestSegment.getValidToSnapshotEpochMillis());
        assertEquals(3, latestSegment.getSampleCount());
    }

    @Test
    void persistSnapshotFlipsDeduplicatesDuplicateFlipKeysInSingleBatch() {
        FlipDefinitionRepository flipDefinitionRepository = mock(FlipDefinitionRepository.class);
        FlipCurrentRepository flipCurrentRepository = mock(FlipCurrentRepository.class);
        FlipTrendSegmentRepository flipTrendSegmentRepository = mock(FlipTrendSegmentRepository.class);
        FlipIdentityService flipIdentityService = mock(FlipIdentityService.class);
        UnifiedFlipDtoMapper unifiedFlipDtoMapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipStorageProperties properties = new FlipStorageProperties();
        properties.setDualWriteEnabled(true);
        UnifiedFlipStorageService service = new UnifiedFlipStorageService(
                flipDefinitionRepository,
                flipCurrentRepository,
                flipTrendSegmentRepository,
                flipIdentityService,
                unifiedFlipDtoMapper,
                contextService,
                properties,
                new ObjectMapper()
        );
        Instant snapshotTimestamp = Instant.parse("2026-02-21T12:00:00Z");
        Flip first = mock(Flip.class);
        Flip second = mock(Flip.class);
        FlipIdentityService.Identity duplicateIdentity = new FlipIdentityService.Identity(
                "key-dupe",
                UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"),
                FlipType.BAZAAR,
                "ENCHANTED_SUGAR",
                "[]",
                "[]",
                1
        );

        when(contextService.loadContextAsOf(snapshotTimestamp)).thenReturn(FlipCalculationContext.standard(null));
        when(flipIdentityService.derive(first)).thenReturn(duplicateIdentity);
        when(flipIdentityService.derive(second)).thenReturn(duplicateIdentity);
        when(unifiedFlipDtoMapper.toDto(any(Flip.class), any(FlipCalculationContext.class))).thenReturn(sampleDto(snapshotTimestamp));
        when(flipDefinitionRepository.findAllById(List.of("key-dupe"))).thenReturn(List.of());
        when(flipCurrentRepository.findAllById(List.of("key-dupe"))).thenReturn(List.of());
        when(flipTrendSegmentRepository.findByFlipKeyInOrderByFlipKeyAscValidToSnapshotEpochMillisDesc(List.of("key-dupe")))
                .thenReturn(List.of());

        service.persistSnapshotFlips(List.of(first, second), snapshotTimestamp);

        ArgumentCaptor<List<FlipDefinitionEntity>> definitionsCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<FlipCurrentEntity>> currentCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<FlipTrendSegmentEntity>> segmentCaptor = ArgumentCaptor.forClass(List.class);
        verify(flipDefinitionRepository).saveAll(definitionsCaptor.capture());
        verify(flipCurrentRepository).saveAll(currentCaptor.capture());
        verify(flipTrendSegmentRepository).saveAll(segmentCaptor.capture());
        assertEquals(1, definitionsCaptor.getValue().size());
        assertEquals(1, currentCaptor.getValue().size());
        assertEquals(1, segmentCaptor.getValue().size());
    }

    private UnifiedFlipDto sampleDto(Instant snapshotTimestamp) {
        return new UnifiedFlipDto(
                UUID.fromString("99999999-9999-9999-9999-999999999999"),
                FlipType.BAZAAR,
                List.of(),
                List.of(),
                1_000_000L,
                100_000L,
                0.2D,
                0.8D,
                120L,
                1_000L,
                80D,
                10D,
                snapshotTimestamp,
                false,
                List.of(),
                List.of(),
                List.of()
        );
    }
}
