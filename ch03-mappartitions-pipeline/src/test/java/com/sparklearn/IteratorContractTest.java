package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class IteratorContractTest {

    @Test
    void filteringIteratorKeepsElementAcrossRepeatedHasNextCalls() {
        FilteringIterator<Integer> iterator = new FilteringIterator<>(
                Arrays.asList(1, 2, 3).iterator(),
                number -> number % 2 == 0);

        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertEquals(2, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    void filteringIteratorSupportsDirectNextAndThrowsWhenExhausted() {
        FilteringIterator<Integer> iterator = new FilteringIterator<>(
                Arrays.asList(1, 2, 3).iterator(),
                number -> number % 2 == 0);

        assertEquals(2, iterator.next());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void filteringIteratorCanReturnNull() {
        FilteringIterator<String> iterator = new FilteringIterator<>(
                Arrays.asList(null, "spark").iterator(),
                Objects::isNull);

        assertTrue(iterator.hasNext());
        assertNull(iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    void flatMapSkipsEmptyChildIterators() {
        List<Integer> result = new ListRDD<>(Arrays.asList(1, 2, 3, 4))
                .flatMap(number -> number % 2 == 0
                        ? Arrays.asList(number, number * 10)
                        : Collections.emptyList())
                .collect();

        assertEquals(Arrays.asList(2, 20, 4, 40), result);
    }

    @Test
    void flatMappingIteratorHandlesRepeatedChecksAndTrailingEmptyLists() {
        FlatMappingIterator<Integer, Integer> iterator = new FlatMappingIterator<>(
                Arrays.asList(1, 2, 3).iterator(),
                number -> number == 2
                        ? Arrays.asList(20, 21)
                        : Collections.emptyList());

        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertEquals(20, iterator.next());
        assertEquals(21, iterator.next());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void transformationsStayLazyUntilCollect() {
        AtomicInteger mapCalls = new AtomicInteger();
        AtomicInteger filterCalls = new AtomicInteger();
        AtomicInteger flatMapCalls = new AtomicInteger();

        RDD<Integer> pipeline = new ListRDD<>(Arrays.asList(1, 2, 3))
                .map(number -> {
                    mapCalls.incrementAndGet();
                    return number + 1;
                })
                .filter(number -> {
                    filterCalls.incrementAndGet();
                    return number % 2 == 0;
                })
                .flatMap(number -> {
                    flatMapCalls.incrementAndGet();
                    return Arrays.asList(number, number * 10);
                });

        assertEquals(0, mapCalls.get());
        assertEquals(0, filterCalls.get());
        assertEquals(0, flatMapCalls.get());

        assertEquals(Arrays.asList(2, 20, 4, 40), pipeline.collect());
        assertEquals(3, mapCalls.get());
        assertEquals(3, filterCalls.get());
        assertEquals(2, flatMapCalls.get());
    }
}
