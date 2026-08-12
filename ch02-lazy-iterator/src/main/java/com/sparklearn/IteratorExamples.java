package com.sparklearn;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 演示 {@link List} 与 {@link Iterator} 的差异。
 */
public final class IteratorExamples {

    private IteratorExamples() {
    }

    public static void main(String[] args) {
        System.out.println("=== 1. 普通 List 遍历 ===");
        plainListForLoop();

        System.out.println("\n=== 2. ArrayList 是房间，Iterator 是门 ===");
        arrayListVsIterator();

        System.out.println("\n=== 3. Iterator 可以代表无限流 ===");
        endlessIterator();
    }

    public static void plainListForLoop() {
        List<String> words = Arrays.asList("hello", "spark", "hello", "world");
        for (String w : words) {
            System.out.println(w);
        }
    }

    public static void arrayListVsIterator() {
        List<String> room = Arrays.asList("hello", "spark", "hello", "world");

        // ArrayList 是一间"房"——数据实实在在存在里面。
        System.out.println("大小：" + room.size());
        System.out.println("第2个：" + room.get(1));
        for (String w : room) {
            System.out.println(w);
        }
        for (String w : room) {
            System.out.println(w);
        }

        // Iterator 只是一道"门"——你只能往前走。
        Iterator<String> door = room.iterator();
        while (door.hasNext()) {
            System.out.println(door.next());
        }
    }

    public static void endlessIterator() {
        Iterator<Integer> endless = new Iterator<>() {
            int n = 0;

            public boolean hasNext() {
                return true;
            }

            public Integer next() {
                return n++;
            }
        };

        System.out.println(endless.next());
        System.out.println(endless.next());
        System.out.println(endless.next());
    }
}
