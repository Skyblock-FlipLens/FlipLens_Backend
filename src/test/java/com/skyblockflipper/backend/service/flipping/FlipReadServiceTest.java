package com.skyblockflipper.backend.service.flipping;

import com.skyblockflipper.backend.api.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.repository.FlipRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FlipReadServiceTest {

    @Test
    void listFlipsWithoutTypeFilterUsesFindAll() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper);

        Flip flip = mock(Flip.class);
        UnifiedFlipDto dto = sampleDto();
        Pageable pageable = PageRequest.of(0, 20);
        when(flipRepository.findAll(pageable)).thenReturn(new PageImpl<>(List.of(flip)));
        when(mapper.toDto(flip)).thenReturn(dto);

        Page<UnifiedFlipDto> result = service.listFlips(null, pageable);

        assertEquals(1, result.getTotalElements());
        assertEquals(dto, result.getContent().getFirst());
        verify(flipRepository).findAll(pageable);
        verify(mapper).toDto(flip);
    }

    @Test
    void listFlipsWithTypeFilterUsesTypeQuery() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper);

        Flip flip = mock(Flip.class);
        UnifiedFlipDto dto = sampleDto();
        Pageable pageable = PageRequest.of(1, 10);
        when(flipRepository.findAllByFlipType(FlipType.BAZAAR, pageable)).thenReturn(new PageImpl<>(List.of(flip)));
        when(mapper.toDto(flip)).thenReturn(dto);

        Page<UnifiedFlipDto> result = service.listFlips(FlipType.BAZAAR, pageable);

        assertEquals(1, result.getTotalElements());
        assertEquals(dto, result.getContent().getFirst());
        verify(flipRepository).findAllByFlipType(FlipType.BAZAAR, pageable);
        verify(mapper).toDto(flip);
    }

    @Test
    void findFlipByIdMapsEntityWhenPresent() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper);

        UUID id = UUID.randomUUID();
        Flip flip = mock(Flip.class);
        UnifiedFlipDto dto = sampleDto();
        when(flipRepository.findById(id)).thenReturn(Optional.of(flip));
        when(mapper.toDto(flip)).thenReturn(dto);

        Optional<UnifiedFlipDto> result = service.findFlipById(id);

        assertTrue(result.isPresent());
        assertEquals(dto, result.get());
        verify(flipRepository).findById(id);
        verify(mapper).toDto(flip);
    }

    @Test
    void findFlipByIdReturnsEmptyWhenMissing() {
        FlipRepository flipRepository = mock(FlipRepository.class);
        UnifiedFlipDtoMapper mapper = mock(UnifiedFlipDtoMapper.class);
        FlipReadService service = new FlipReadService(flipRepository, mapper);

        UUID id = UUID.randomUUID();
        when(flipRepository.findById(id)).thenReturn(Optional.empty());

        Optional<UnifiedFlipDto> result = service.findFlipById(id);

        assertTrue(result.isEmpty());
        verify(flipRepository).findById(id);
    }

    private UnifiedFlipDto sampleDto() {
        return new UnifiedFlipDto(
                UUID.randomUUID(),
                FlipType.BAZAAR,
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
                Instant.now(),
                List.of(),
                List.of()
        );
    }
}
