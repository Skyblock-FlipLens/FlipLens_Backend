package com.skyblockflipper.backend.api;

import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.service.flipping.FlipReadService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(FlipController.class)
class FlipControllerWebMvcTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private FlipReadService flipReadService;

    @Test
    void listFlipsReturnsPageAndInvokesServiceWithExpectedPageableAndFlipType() throws Exception {
        UUID flipId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        UnifiedFlipDto dto = new UnifiedFlipDto(
                flipId,
                FlipType.FORGE,
                List.of(new UnifiedFlipDto.ItemStackDto("ENCHANTED_DIAMOND", 2)),
                List.of(new UnifiedFlipDto.ItemStackDto("REFINED_DIAMOND", 1)),
                150_000L,
                null,
                null,
                null,
                3_600L,
                null,
                null,
                null,
                Instant.parse("2026-02-16T00:00:00Z"),
                List.of(),
                List.of()
        );

        PageRequest expectedRequest = PageRequest.of(1, 2, Sort.by("id").ascending());
        Page<UnifiedFlipDto> resultPage = new PageImpl<>(List.of(dto), expectedRequest, 1);
        when(flipReadService.listFlips(eq(FlipType.FORGE), any(Pageable.class))).thenReturn(resultPage);

        mockMvc.perform(get("/api/v1/flips")
                        .param("flipType", "FORGE")
                        .param("page", "1")
                        .param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.content.length()").value(1))
                .andExpect(jsonPath("$.content[0].id").value(flipId.toString()))
                .andExpect(jsonPath("$.content[0].flipType").value("FORGE"))
                .andExpect(jsonPath("$.content[0].inputItems[0].itemId").value("ENCHANTED_DIAMOND"))
                .andExpect(jsonPath("$.content[0].outputItems[0].itemId").value("REFINED_DIAMOND"))
                .andExpect(jsonPath("$.number").value(1))
                .andExpect(jsonPath("$.size").value(2))
                .andExpect(jsonPath("$.totalElements").value(3))
                .andExpect(jsonPath("$.totalPages").value(2));

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        ArgumentCaptor<FlipType> flipTypeCaptor = ArgumentCaptor.forClass(FlipType.class);
        verify(flipReadService, times(1)).listFlips(flipTypeCaptor.capture(), pageableCaptor.capture());

        assertEquals(FlipType.FORGE, flipTypeCaptor.getValue());
        Pageable pageable = pageableCaptor.getValue();
        assertEquals(1, pageable.getPageNumber());
        assertEquals(2, pageable.getPageSize());
        assertEquals(Sort.by("id").ascending(), pageable.getSort());
    }
}
