package com.skyblockflipper.backend.api;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Pageable implementation that uses an absolute offset and limit.
 */
public final class OffsetLimitPageRequest implements Pageable, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final long offset;
    private final int pageSize;
    private final Sort sort;

    private OffsetLimitPageRequest(long offset, int pageSize, Sort sort) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must be non-negative.");
        }
        if (pageSize < 1) {
            throw new IllegalArgumentException("Page size must be at least 1.");
        }
        this.offset = offset;
        this.pageSize = pageSize;
        this.sort = sort == null ? Sort.unsorted() : sort;
    }

    public static OffsetLimitPageRequest of(long offset, int pageSize, Sort sort) {
        return new OffsetLimitPageRequest(offset, pageSize, sort);
    }

    @Override
    public int getPageNumber() {
        return (int) (offset / pageSize);
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public Sort getSort() {
        return sort;
    }

    @Override
    public Pageable next() {
        return new OffsetLimitPageRequest(offset + pageSize, pageSize, sort);
    }

    @Override
    public Pageable previousOrFirst() {
        if (!hasPrevious()) {
            return first();
        }
        return new OffsetLimitPageRequest(Math.max(0, offset - pageSize), pageSize, sort);
    }

    @Override
    public Pageable first() {
        return new OffsetLimitPageRequest(0, pageSize, sort);
    }

    @Override
    public Pageable withPage(int pageNumber) {
        if (pageNumber < 0) {
            throw new IllegalArgumentException("Page number must be non-negative.");
        }
        return new OffsetLimitPageRequest((long) pageNumber * pageSize, pageSize, sort);
    }

    @Override
    public boolean hasPrevious() {
        return offset > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OffsetLimitPageRequest that)) {
            return false;
        }
        return offset == that.offset && pageSize == that.pageSize && Objects.equals(sort, that.sort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, pageSize, sort);
    }
}
