package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.service.flipping.FlipReadService;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlipControllerTest {

    @Test
    void listFlipsDelegatesToService() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        Pageable pageable = PageRequest.of(0, 50);
        UnifiedFlipDto dto = sampleDto();
        Page<UnifiedFlipDto> expected = new PageImpl<>(List.of(dto), pageable, 1);

        when(service.listFlips(FlipType.FORGE, pageable)).thenReturn(expected);

        Page<UnifiedFlipDto> response = controller.listFlips(FlipType.FORGE, pageable);

        assertEquals(expected, response);
        verify(service).listFlips(FlipType.FORGE, pageable);
    }

    @Test
    void getFlipByIdReturnsOkWhenFound() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        UUID id = UUID.randomUUID();
        UnifiedFlipDto dto = sampleDto();

        when(service.findFlipById(id)).thenReturn(Optional.of(dto));

        ResponseEntity<UnifiedFlipDto> response = controller.getFlipById(id);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(dto, response.getBody());
        verify(service).findFlipById(id);
    }

    @Test
    void getFlipByIdReturns404WhenMissing() {
        FlipReadService service = mock(FlipReadService.class);
        FlipController controller = new FlipController(service);
        UUID id = UUID.randomUUID();

        when(service.findFlipById(id)).thenReturn(Optional.empty());

        ResponseEntity<UnifiedFlipDto> response = controller.getFlipById(id);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        verify(service).findFlipById(id);
    }

    private UnifiedFlipDto sampleDto() {
        return new UnifiedFlipDto(
                UUID.randomUUID(),
                FlipType.FORGE,
                List.of(),
                List.of(),
                null,
                null,
                null,
                null,
                120L,
                null,
                null,
                null,
                Instant.now(),
                false,
                List.of(),
                List.of(),
                List.of()
        );
    }
}
