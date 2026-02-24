package com.skyblockflipper.backend.api;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

public final class StandardPagination {

    private static final int MAX_PAGE_SIZE = 1_000;

    private StandardPagination() {
    }

    public static Pageable pageable(Integer page, Integer size, int defaultSize, Sort sort) {
        int safeDefaultSize = Math.max(1, defaultSize);
        int safePage = page == null ? 0 : Math.max(0, page);
        int requestedSize = size == null ? safeDefaultSize : Math.max(1, size);
        int safeSize = Math.min(MAX_PAGE_SIZE, requestedSize);
        Sort safeSort = sort == null ? Sort.unsorted() : sort;
        return PageRequest.of(safePage, safeSize, safeSort);
    }
}
