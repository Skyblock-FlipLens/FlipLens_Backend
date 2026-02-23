package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.repository.FlipRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContext;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlipStorageParityServiceTest {

    @Test
    void latestParityReportTreatsNullAndZeroAsDifferentForNumericFields() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        UnifiedFlipDtoMapper dtoMapper = mock(UnifiedFlipDtoMapper.class);
        UnifiedFlipCurrentReadService currentReadService = mock(UnifiedFlipCurrentReadService.class);
        FlipIdentityService identityService = mock(FlipIdentityService.class);
        FlipStorageProperties properties = new FlipStorageProperties();
        FlipStorageParityService service = new FlipStorageParityService(
                flipRepository,
                contextService,
                dtoMapper,
                currentReadService,
                identityService,
                properties
        );

        long snapshotEpochMillis = Instant.parse("2026-02-20T12:00:00Z").toEpochMilli();
        Instant snapshotTimestamp = Instant.ofEpochMilli(snapshotEpochMillis);
        UUID persistedFlipId = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        UUID stableFlipId = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        Flip legacyFlip = mock(Flip.class);
        when(legacyFlip.getId()).thenReturn(persistedFlipId);

        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.of(snapshotEpochMillis));
        when(currentReadService.latestSnapshotEpochMillis()).thenReturn(Optional.of(snapshotEpochMillis));
        when(flipRepository.findAllBySnapshotTimestampEpochMillis(snapshotEpochMillis)).thenReturn(List.of(legacyFlip));
        FlipCalculationContext context = FlipCalculationContext.standard(null);
        when(contextService.loadContextAsOf(snapshotTimestamp)).thenReturn(context);
        when(dtoMapper.toDto(legacyFlip, context)).thenReturn(sampleDto(UUID.randomUUID(), snapshotTimestamp, null));
        when(identityService.derive(legacyFlip)).thenReturn(new FlipIdentityService.Identity(
                "v1_key",
                stableFlipId,
                FlipType.BAZAAR,
                "ENCHANTED_SUGAR",
                "[]",
                "[]",
                1
        ));
        when(currentReadService.listCurrent(null)).thenReturn(List.of(sampleDto(stableFlipId, snapshotTimestamp, 0L)));

        FlipStorageParityService.FlipStorageParityReport report = service.latestParityReport();

        assertEquals(1L, report.metricMismatchCount());
        assertEquals(1, report.sampleMetricMismatches().size());
        assertEquals(stableFlipId, report.sampleMetricMismatches().getFirst().flipId());
        assertEquals(List.of("expectedProfit"), report.sampleMetricMismatches().getFirst().mismatchedFields());
    }

    @Test
    void latestParityReportMemoizesStableIdDerivationPerPersistedId() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        UnifiedFlipDtoMapper dtoMapper = mock(UnifiedFlipDtoMapper.class);
        UnifiedFlipCurrentReadService currentReadService = mock(UnifiedFlipCurrentReadService.class);
        FlipIdentityService identityService = mock(FlipIdentityService.class);
        FlipStorageProperties properties = new FlipStorageProperties();
        FlipStorageParityService service = new FlipStorageParityService(
                flipRepository,
                contextService,
                dtoMapper,
                currentReadService,
                identityService,
                properties
        );

        long snapshotEpochMillis = Instant.parse("2026-02-20T12:00:00Z").toEpochMilli();
        Instant snapshotTimestamp = Instant.ofEpochMilli(snapshotEpochMillis);
        UUID persistedFlipId = UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc");
        UUID stableFlipId = UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd");
        Flip legacyFlipA = mock(Flip.class);
        Flip legacyFlipB = mock(Flip.class);
        when(legacyFlipA.getId()).thenReturn(persistedFlipId);
        when(legacyFlipB.getId()).thenReturn(persistedFlipId);

        when(flipRepository.findMaxSnapshotTimestampEpochMillis()).thenReturn(Optional.of(snapshotEpochMillis));
        when(currentReadService.latestSnapshotEpochMillis()).thenReturn(Optional.of(snapshotEpochMillis));
        when(flipRepository.findAllBySnapshotTimestampEpochMillis(snapshotEpochMillis)).thenReturn(List.of(legacyFlipA, legacyFlipB));
        FlipCalculationContext context = FlipCalculationContext.standard(null);
        when(contextService.loadContextAsOf(snapshotTimestamp)).thenReturn(context);
        when(dtoMapper.toDto(legacyFlipA, context)).thenReturn(sampleDto(UUID.randomUUID(), snapshotTimestamp, 200L));
        when(dtoMapper.toDto(legacyFlipB, context)).thenReturn(sampleDto(UUID.randomUUID(), snapshotTimestamp, 200L));
        when(identityService.derive(any(Flip.class))).thenReturn(new FlipIdentityService.Identity(
                "v1_key",
                stableFlipId,
                FlipType.BAZAAR,
                "ENCHANTED_SUGAR",
                "[]",
                "[]",
                1
        ));
        when(currentReadService.listCurrent(null)).thenReturn(List.of(sampleDto(stableFlipId, snapshotTimestamp, 200L)));

        service.latestParityReport();

        verify(identityService, times(1)).derive(any(Flip.class));
    }

    private UnifiedFlipDto sampleDto(UUID id, Instant snapshotTimestamp, Long expectedProfit) {
        return new UnifiedFlipDto(
                id,
                FlipType.BAZAAR,
                List.of(),
                List.of(),
                1_000_000L,
                expectedProfit,
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

