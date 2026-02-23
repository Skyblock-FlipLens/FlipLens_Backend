package com.skyblockflipper.backend.api;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

public final class RangePagination {

    private static final int MAX_PAGE_SIZE = 1_000;

    private RangePagination() {
    }

    public static Pageable pageable(Integer min, Integer max, int defaultSize, Sort sort) {
        long safeDefaultSize = Math.max(1L, (long) defaultSize);
        long safeMin = min == null ? 0L : Math.max(0L, min.longValue());
        long safeMax = max == null ? safeMin + safeDefaultSize - 1L : Math.max(safeMin, max.longValue());
        long size = safeMax - safeMin + 1L;
        long clampedSizeLong = Math.max(1L, Math.min((long) MAX_PAGE_SIZE, size));
        int clampedSize = (int) clampedSizeLong;
        return OffsetLimitPageRequest.of(safeMin, clampedSize, sort);
    }
}
