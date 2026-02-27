package com.skyblockflipper.backend.api;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OffsetLimitPageRequestTest {

    @Test
    void ofRejectsNegativeOffsetAndInvalidPageSize() {
        assertThrows(IllegalArgumentException.class, () -> OffsetLimitPageRequest.of(-1L, 10, Sort.unsorted()));
        assertThrows(IllegalArgumentException.class, () -> OffsetLimitPageRequest.of(0L, 0, Sort.unsorted()));
    }

    @Test
    void navigationMethodsComputeExpectedOffsets() {
        OffsetLimitPageRequest request = OffsetLimitPageRequest.of(30L, 10, Sort.by("id").descending());

        Pageable next = request.next();
        Pageable previous = request.previousOrFirst();
        Pageable first = request.first();
        Pageable pageTwo = request.withPage(2);

        assertEquals(3, request.getPageNumber());
        assertEquals(10, request.getPageSize());
        assertEquals(30L, request.getOffset());
        assertEquals(40L, next.getOffset());
        assertEquals(20L, previous.getOffset());
        assertEquals(0L, first.getOffset());
        assertEquals(20L, pageTwo.getOffset());
        assertTrue(request.hasPrevious());
        assertEquals(Sort.by("id").descending(), request.getSort());
    }

    @Test
    void previousOrFirstReturnsFirstWhenNoPreviousExists() {
        OffsetLimitPageRequest request = OffsetLimitPageRequest.of(0L, 25, null);

        Pageable previousOrFirst = request.previousOrFirst();

        assertEquals(0L, previousOrFirst.getOffset());
        assertEquals(25, previousOrFirst.getPageSize());
        assertFalse(previousOrFirst.hasPrevious());
        assertEquals(Sort.unsorted(), previousOrFirst.getSort());
    }

    @Test
    void withPageRejectsNegativePageNumber() {
        OffsetLimitPageRequest request = OffsetLimitPageRequest.of(0L, 10, Sort.unsorted());
        assertThrows(IllegalArgumentException.class, () -> request.withPage(-1));
    }

    @Test
    void equalsAndHashCodeUseAllFields() {
        OffsetLimitPageRequest a = OffsetLimitPageRequest.of(10L, 5, Sort.by("x").ascending());
        OffsetLimitPageRequest b = OffsetLimitPageRequest.of(10L, 5, Sort.by("x").ascending());
        OffsetLimitPageRequest c = OffsetLimitPageRequest.of(15L, 5, Sort.by("x").ascending());

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(c));
    }
}
