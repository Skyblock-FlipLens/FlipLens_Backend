package com.skyblockflipper.backend.service.item;

import com.skyblockflipper.backend.NEU.model.Item;
import com.skyblockflipper.backend.NEU.repository.ItemRepository;
import com.skyblockflipper.backend.api.ItemDto;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Locale;

@Service
public class ItemReadService {

    private final ItemRepository itemRepository;

    public ItemReadService(ItemRepository itemRepository) {
        this.itemRepository = itemRepository;
    }

    @Transactional(readOnly = true)
    public Page<ItemDto> listItems(String itemId, Pageable pageable) {
        String normalizedItemId = normalize(itemId);
        if (normalizedItemId.isEmpty()) {
            return itemRepository.findAll(pageable).map(this::toDto);
        }

        return itemRepository.findById(normalizedItemId)
                .map(item -> new PageImpl<>(List.of(toDto(item)), pageable, 1))
                .orElseGet(() -> new PageImpl<>(List.of(), pageable, 0));
    }

    private ItemDto toDto(Item item) {
        return new ItemDto(
                item.getId(),
                item.getDisplayName(),
                item.getMinecraftId(),
                item.getRarity(),
                item.getCategory(),
                item.getInfoLinks()
        );
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }
}
