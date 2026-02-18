package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.service.flipping.FlipReadService;
import com.skyblockflipper.backend.service.market.MarketSnapshotReadService;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SnapshotControllerTest {

    @Test
    void listSnapshotsDelegatesToService() {
        MarketSnapshotReadService snapshotReadService = mock(MarketSnapshotReadService.class);
        FlipReadService flipReadService = mock(FlipReadService.class);
        SnapshotController controller = new SnapshotController(snapshotReadService, flipReadService);
        Pageable pageable = PageRequest.of(0, 100);
        MarketSnapshotDto dto = new MarketSnapshotDto(
                UUID.randomUUID(),
                Instant.parse("2026-02-18T21:00:00Z"),
                100,
                2000,
                Instant.parse("2026-02-18T21:00:03Z")
        );
        Page<MarketSnapshotDto> expected = new PageImpl<>(List.of(dto), pageable, 1);

        when(snapshotReadService.listSnapshots(pageable)).thenReturn(expected);

        Page<MarketSnapshotDto> response = controller.listSnapshots(pageable);

        assertEquals(expected, response);
        verify(snapshotReadService).listSnapshots(pageable);
    }

    @Test
    void listFlipsForSnapshotDelegatesToFlipReadService() {
        MarketSnapshotReadService snapshotReadService = mock(MarketSnapshotReadService.class);
        FlipReadService flipReadService = mock(FlipReadService.class);
        SnapshotController controller = new SnapshotController(snapshotReadService, flipReadService);
        Pageable pageable = PageRequest.of(0, 50);
        long snapshotEpochMillis = Instant.parse("2026-02-18T21:00:00Z").toEpochMilli();
        UnifiedFlipDto dto = new UnifiedFlipDto(
                UUID.randomUUID(),
                FlipType.CRAFTING,
                List.of(),
                List.of(),
                null,
                null,
                null,
                null,
                0L,
                null,
                null,
                null,
                Instant.ofEpochMilli(snapshotEpochMillis),
                false,
                List.of(),
                List.of(),
                List.of()
        );
        Page<UnifiedFlipDto> expected = new PageImpl<>(List.of(dto), pageable, 1);

        when(flipReadService.listFlips(FlipType.CRAFTING, Instant.ofEpochMilli(snapshotEpochMillis), pageable))
                .thenReturn(expected);

        Page<UnifiedFlipDto> response = controller.listFlipsForSnapshot(snapshotEpochMillis, FlipType.CRAFTING, pageable);

        assertEquals(expected, response);
        verify(flipReadService).listFlips(FlipType.CRAFTING, Instant.ofEpochMilli(snapshotEpochMillis), pageable);
    }
}
