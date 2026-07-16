package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

final class DependencyContractTest {

    @Test
    void listRddHasOnePartitionAndNoDependencies() {
        ListRDD<Integer> rdd = new ListRDD<>(Arrays.asList(1, 2, 3));

        assertEquals(List.of(new Partition(0)), rdd.partitions());
        assertEquals(List.of(), rdd.dependencies());
        assertEquals(List.of(1, 2, 3), rdd.collect());
    }

    @Test
    void mapPartitionsRddKeepsParentPartitionsAndOneToOneDependency() {
        ListRDD<Integer> parent = new ListRDD<>(Arrays.asList(1, 2, 3));
        RDD<Integer> mapped = parent.map(number -> number + 1);

        assertEquals(parent.partitions(), mapped.partitions());
        assertEquals(1, mapped.dependencies().size());

        Dependency<?> dependency = mapped.dependencies().get(0);
        OneToOneDependency<?> oneToOneDependency =
                assertInstanceOf(OneToOneDependency.class, dependency);
        assertSame(parent, oneToOneDependency.rdd());
        assertEquals(List.of(0), oneToOneDependency.getParents(0));
    }

    @Test
    void lineageCanBeWalkedBackToSource() {
        RDD<Integer> source = new ListRDD<>(Arrays.asList(1, 2, 3, 4));
        RDD<Integer> pipeline = source
                .map(number -> number + 1)
                .filter(number -> number % 2 == 0)
                .map(number -> number * 10);

        RDD<?> current = pipeline;
        int depth = 0;
        while (!current.dependencies().isEmpty()) {
            Dependency<?> dependency = current.dependencies().get(0);
            assertInstanceOf(OneToOneDependency.class, dependency);
            current = dependency.rdd();
            depth++;
        }

        assertSame(source, current);
        assertEquals(3, depth);
        assertEquals(List.of(20, 40), pipeline.collect());
    }

    @Test
    void countAndReduceConsumeAllPartitions() {
        RDD<Integer> rdd = new ListRDD<>(Arrays.asList(1, 2, 3, 4, 5))
                .map(number -> number * 2);

        assertEquals(5, rdd.count());
        assertEquals(30, rdd.reduce(Integer::sum));
    }

    @Test
    void reduceOnEmptyRddFailsClearly() {
        RDD<Integer> rdd = new ListRDD<>(List.of());

        assertThrows(NoSuchElementException.class, () -> rdd.reduce(Integer::sum));
    }
}
