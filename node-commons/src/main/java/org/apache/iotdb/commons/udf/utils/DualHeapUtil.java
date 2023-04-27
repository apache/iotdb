package Util;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class DualHeap {
    private final PriorityQueue<Double> small;  // max-heap
    private final PriorityQueue<Double> large;  // min-heap
    private final Map<Double, Integer> delayed;  // delay to delete hashset

    private int smallSize, largeSize;  // size of heap(without the delayed)

    public DualHeap() {
        this.small = new PriorityQueue<>(Comparator.reverseOrder());
        this.large = new PriorityQueue<>(Comparator.naturalOrder());
        this.delayed = new HashMap<>();
        this.smallSize = 0;
        this.largeSize = 0;
    }

    public double getMedian() {
        if (small.isEmpty()) return -1;  // error
        return ((smallSize + largeSize) & 1) == 1 ? small.peek() : ((double) small.peek() + large.peek()) / 2;
    }

    public void insert(double num) {
        if (small.isEmpty() || num <= small.peek()) {
            small.offer(num);
            ++smallSize;
        } else {
            large.offer(num);
            ++largeSize;
        }
        makeBalance();
    }

    public void erase(double num) {
        delayed.put(num, delayed.getOrDefault(num, 0) + 1);
        if (num <= small.peek()) {
            --smallSize;
            if (num == small.peek()) {
                prune(small);
            }
        } else {
            --largeSize;
            if (num == large.peek()) {
                prune(large);
            }
        }
        makeBalance();
    }

    public void clear() {
        small.clear();
        large.clear();
        delayed.clear();
        smallSize = 0;
        largeSize = 0;
    }

    private void prune(PriorityQueue<Double> heap) {
        while (!heap.isEmpty()) {
            double num = heap.peek();
            if (delayed.containsKey(num)) {
                delayed.put(num, delayed.get(num) - 1);
                if (delayed.get(num) == 0) {
                    delayed.remove(num);
                }
                heap.poll();
            } else {
                break;
            }
        }
    }

    private void makeBalance() {
        if (smallSize > largeSize + 1) {
            large.offer(small.poll());
            --smallSize;
            ++largeSize;
            prune(small);
        } else if (smallSize < largeSize) {
            small.offer(large.poll());
            ++smallSize;
            --largeSize;
            prune(large);
        }
    }
}