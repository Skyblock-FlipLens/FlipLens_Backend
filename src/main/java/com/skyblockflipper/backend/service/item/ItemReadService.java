package com.skyblockflipper.backend.service.item;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.ItemDto;
import com.skyblockflipper.backend.api.MarketplaceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

@Service
public class ItemReadService {

    private final ItemRepository itemRepository;
    private final ItemMarketplaceService itemMarketplaceService;

    public ItemReadService(ItemRepository itemRepository) {
        this(itemRepository, null);
    }

    @Autowired
    public ItemReadService(ItemRepository itemRepository, ItemMarketplaceService itemMarketplaceService) {
        this.itemRepository = itemRepository;
        this.itemMarketplaceService = itemMarketplaceService;
    }

    @Transactional(readOnly = true)
    public Page<ItemDto> listItems(String itemId, Pageable pageable) {
        return listItems(itemId, null, null, null, null, pageable);
    }

    @Transactional(readOnly = true)
    public Page<ItemDto> listItems(String itemId,
                                   String search,
                                   String category,
                                   String rarity,
                                   MarketplaceType marketplace,
                                   Pageable pageable) {
        String normalizedSearch = normalize(search != null && !search.isBlank() ? search : itemId);
        String normalizedCategory = normalize(category);
        String normalizedRarity = normalize(rarity);

        Sort sort = pageable != null && pageable.getSort().isSorted() ? pageable.getSort() : Sort.by("id").ascending();
        List<Item> all;
        try {
            all = itemRepository.findAll(sort);
        } catch (RuntimeException ignored) {
            all = itemRepository.findAll(Sort.by("id").ascending());
        }

        Map<String, MarketplaceType> marketplaces = resolveMarketplaces(all);
        List<ItemDto> filtered = all.stream()
                .filter(item -> matchesSearch(item, normalizedSearch))
                .filter(item -> normalizedCategory.isEmpty() || normalizedCategory.equalsIgnoreCase(normalize(item.getCategory())))
                .filter(item -> normalizedRarity.isEmpty() || normalizedRarity.equalsIgnoreCase(normalize(item.getRarity())))
                .map(item -> toDto(item, marketplaces.getOrDefault(item.getId(), MarketplaceType.NONE)))
                .filter(dto -> marketplace == null || marketplace == dto.marketplace())
                .toList();

        return paginate(filtered, pageable);
    }

    @Transactional(readOnly = true)
    public Optional<ItemDto> findItemById(String itemId) {
        String normalized = normalize(itemId);
        if (normalized.isEmpty()) {
            return Optional.empty();
        }
        return itemRepository.findById(normalized)
                .map(item -> {
                    Map<String, MarketplaceType> marketplaces = resolveMarketplaces(List.of(item));
                    return toDto(item, marketplaces.getOrDefault(item.getId(), MarketplaceType.NONE));
                });
    }

    private ItemDto toDto(Item item, MarketplaceType marketplace) {
        return new ItemDto(
                item.getId(),
                item.getDisplayName(),
                item.getMinecraftId(),
                item.getRarity(),
                item.getCategory(),
                marketplace,
                item.getInfoLinks()
        );
    }

    private boolean matchesSearch(Item item, String search) {
        if (search.isEmpty()) {
            return true;
        }
        String id = normalize(item.getId());
        String displayName = normalize(item.getDisplayName());
        String minecraftId = normalize(item.getMinecraftId());
        return id.contains(search) || displayName.contains(search) || minecraftId.contains(search);
    }

    private Map<String, MarketplaceType> resolveMarketplaces(List<Item> items) {
        if (itemMarketplaceService == null) {
            return Map.of();
        }
        return itemMarketplaceService.resolveMarketplaces(items);
    }

    private Page<ItemDto> paginate(List<ItemDto> values, Pageable pageable) {
        if (pageable == null || pageable.isUnpaged()) {
            return new PageImpl<>(new ArrayList<>(values));
        }
        int fromIndex = (int) Math.min((long) pageable.getPageNumber() * pageable.getPageSize(), values.size());
        int toIndex = Math.min(fromIndex + pageable.getPageSize(), values.size());
        List<ItemDto> content = fromIndex >= toIndex ? List.of() : values.subList(fromIndex, toIndex);
        return new PageImpl<>(content, pageable, values.size());
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }
}
