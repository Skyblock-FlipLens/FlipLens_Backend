package com.skyblockflipper.backend.service.flipping.storage;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.api.dto.UnifiedFlipDto;
import com.skyblockflipper.backend.model.Flipping.Enums.FlipType;
import com.skyblockflipper.backend.model.Flipping.Flip;
import com.skyblockflipper.backend.model.Flipping.Recipe.Recipe;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeIngredient;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeProcessType;
import com.skyblockflipper.backend.model.Flipping.Recipe.RecipeToFlipMapper;
import com.skyblockflipper.backend.model.market.MarketSnapshot;
import com.skyblockflipper.backend.model.market.UnifiedFlipInputSnapshot;
import com.skyblockflipper.backend.repository.RecipeRepository;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContext;
import com.skyblockflipper.backend.service.flipping.FlipCalculationContextService;
import com.skyblockflipper.backend.service.flipping.MarketFlipMapper;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipDtoMapper;
import com.skyblockflipper.backend.service.flipping.UnifiedFlipInputMapper;
import com.skyblockflipper.backend.service.market.MarketSnapshotPersistenceService;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Sort;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OnDemandFlipSnapshotServiceTest {

    @Test
    void computeSnapshotDtosReturnsEmptyWhenSnapshotIsNull() {
        RecipeRepository recipeRepository = mock(RecipeRepository.class);
        RecipeToFlipMapper recipeToFlipMapper = mock(RecipeToFlipMapper.class);
        MarketSnapshotPersistenceService marketSnapshotPersistenceService = mock(MarketSnapshotPersistenceService.class);
        UnifiedFlipInputMapper unifiedFlipInputMapper = mock(UnifiedFlipInputMapper.class);
        MarketFlipMapper marketFlipMapper = mock(MarketFlipMapper.class);
        UnifiedFlipDtoMapper unifiedFlipDtoMapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipIdentityService identityService = mock(FlipIdentityService.class);
        OnDemandFlipSnapshotService service = new OnDemandFlipSnapshotService(
                recipeRepository,
                recipeToFlipMapper,
                marketSnapshotPersistenceService,
                unifiedFlipInputMapper,
                marketFlipMapper,
                unifiedFlipDtoMapper,
                contextService,
                identityService
        );

        List<UnifiedFlipDto> result = service.computeSnapshotDtos(null, null);

        assertTrue(result.isEmpty());
        verify(recipeRepository, never()).findAll(any(Sort.class));
    }

    @Test
    void computeSnapshotDtosBuildsAndFiltersDtosUsingStableIds() {
        RecipeRepository recipeRepository = mock(RecipeRepository.class);
        RecipeToFlipMapper recipeToFlipMapper = mock(RecipeToFlipMapper.class);
        MarketSnapshotPersistenceService marketSnapshotPersistenceService = mock(MarketSnapshotPersistenceService.class);
        UnifiedFlipInputMapper unifiedFlipInputMapper = mock(UnifiedFlipInputMapper.class);
        MarketFlipMapper marketFlipMapper = mock(MarketFlipMapper.class);
        UnifiedFlipDtoMapper unifiedFlipDtoMapper = mock(UnifiedFlipDtoMapper.class);
        FlipCalculationContextService contextService = mock(FlipCalculationContextService.class);
        FlipIdentityService identityService = mock(FlipIdentityService.class);
        OnDemandFlipSnapshotService service = new OnDemandFlipSnapshotService(
                recipeRepository,
                recipeToFlipMapper,
                marketSnapshotPersistenceService,
                unifiedFlipInputMapper,
                marketFlipMapper,
                unifiedFlipDtoMapper,
                contextService,
                identityService
        );

        Instant snapshot = Instant.parse("2026-02-20T12:00:00Z");
        FlipCalculationContext context = FlipCalculationContext.standard(null);
        when(contextService.loadContextAsOf(snapshot)).thenReturn(context);
        Item outputItem = Item.builder().id("ENCHANTED_HAY_BALE").build();
        Recipe recipe = new Recipe(
                "ENCHANTED_HAY_BALE:craft:0",
                outputItem,
                RecipeProcessType.CRAFT,
                0L,
                List.of(new RecipeIngredient("HAY_BLOCK", 144))
        );
        when(recipeRepository.findAll(any(Sort.class))).thenReturn(List.of(recipe));

        Flip craftingFlip = new Flip(null, FlipType.CRAFTING, List.of(), "ENCHANTED_HAY_BALE", List.of());
        Flip bazaarFlip = new Flip(null, FlipType.BAZAAR, List.of(), "ENCHANTED_SUGAR", List.of());
        when(recipeToFlipMapper.fromRecipe(recipe)).thenReturn(craftingFlip);
        MarketSnapshot marketSnapshot = new MarketSnapshot(snapshot, List.of(), Map.of());
        UnifiedFlipInputSnapshot inputSnapshot = new UnifiedFlipInputSnapshot(snapshot, Map.of(), Map.of());
        when(marketSnapshotPersistenceService.asOf(snapshot)).thenReturn(Optional.of(marketSnapshot));
        when(unifiedFlipInputMapper.map(marketSnapshot)).thenReturn(inputSnapshot);
        when(marketFlipMapper.fromMarketSnapshot(inputSnapshot)).thenReturn(List.of(bazaarFlip));

        UnifiedFlipDto craftingDto = dto(UUID.fromString("11111111-1111-1111-1111-111111111111"), FlipType.CRAFTING, snapshot);
        UnifiedFlipDto bazaarDto = dto(UUID.fromString("22222222-2222-2222-2222-222222222222"), FlipType.BAZAAR, snapshot);
        when(unifiedFlipDtoMapper.toDto(craftingFlip, context)).thenReturn(craftingDto);
        when(unifiedFlipDtoMapper.toDto(bazaarFlip, context)).thenReturn(bazaarDto);
        when(identityService.derive(craftingFlip)).thenReturn(new FlipIdentityService.Identity(
                "k1",
                UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                FlipType.CRAFTING,
                "ENCHANTED_HAY_BALE",
                "[]",
                "[]",
                1
        ));
        when(identityService.derive(bazaarFlip)).thenReturn(new FlipIdentityService.Identity(
                "k2",
                UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
                FlipType.BAZAAR,
                "ENCHANTED_SUGAR",
                "[]",
                "[]",
                1
        ));

        List<UnifiedFlipDto> result = service.computeSnapshotDtos(snapshot, FlipType.BAZAAR);

        assertEquals(1, result.size());
        assertEquals(FlipType.BAZAAR, result.getFirst().flipType());
        assertEquals(UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"), result.getFirst().id());
        assertEquals(snapshot.toEpochMilli(), craftingFlip.getSnapshotTimestampEpochMillis());
        assertEquals(snapshot.toEpochMilli(), bazaarFlip.getSnapshotTimestampEpochMillis());
    }

    private UnifiedFlipDto dto(UUID id, FlipType flipType, Instant snapshotTimestamp) {
        return new UnifiedFlipDto(
                id,
                flipType,
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

