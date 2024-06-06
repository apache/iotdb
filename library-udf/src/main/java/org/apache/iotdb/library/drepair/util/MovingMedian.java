package org.apache.iotdb.library.drepair.util;

// period must be even
public class MovingMedian {

    private final int windowSize;

    /* The current size of the window */
    private int size;

    /* Position of the oldest element of the window */
    private int startIndex;

    /*
     * Each element of windowHeap is a pointer to either maxHeap or minHeap.
     * Each element of windowHeapIndex is a index of the element in the corresponding Heap.
     * Together they represent the moving window, such that the k-th element of the window
     * can be accessed using windowHeap[k].heap[window[k]]
     * */
    private final Heap[] windowHeap;
    private final int[] window;

    private final Heap maxHeap;
    private final Heap minHeap;
    private double median;

    public MovingMedian(int windowSize) {
        this.windowSize = windowSize;
        this.maxHeap = new Heap(windowSize / 2, false);
        this.minHeap = new Heap(windowSize / 2, true);
        window = new int[windowSize];
        windowHeap = new Heap[windowSize];
    }

    private int inc(int k) {
        return ++k == windowSize ? 0 : k;
    }

    private void calcMedian() {
        median = (minHeap.top() + maxHeap.top()) / 2;
    }

    public double getMedian() {
        return median;
    }

    public void update(double value) {
        if (size >= windowSize) {
            windowHeap[startIndex].extract(window[startIndex]);
            size--;
        }

        if (value <= maxHeap.top()) {
            if (maxHeap.size == maxHeap.cap || (maxHeap.size - minHeap.size) >= 1) {
                maxHeap.transfer(0, minHeap);
            }
            maxHeap.insert(value, startIndex);
        } else {
            if (minHeap.size == minHeap.cap || (minHeap.size - maxHeap.size) >= 1) {
                minHeap.transfer(0, maxHeap);
            }
            minHeap.insert(value, startIndex);
        }

        while (minHeap.size > 2 && maxHeap.top() > minHeap.top()) {
            maxHeap.exchange(0, minHeap, 0);
        }
        size++;
        startIndex = inc(startIndex);
        calcMedian();
    }

    class Heap {
        private int size = 0;
        private final double[] heap;
        // contains the inverted index of each element in the heap to their positions in the window
        // such that,  heap[i] == heap[window[invertedIndex[i]]].
        private final int[] invertedIndex;
        private final boolean minHeap;
        private final int cap;

        public Heap(int cap, boolean minHeap) {
            this.cap = cap;
            this.heap = new double[cap + 1];
            this.invertedIndex = new int[cap + 1];
            this.minHeap = minHeap;
        }

        private double top() {
            return heap[0];
        }

        void insert(double value, int qIndex) {
            heap[size] = value;
            window[qIndex] = size;
            windowHeap[qIndex] = this;
            invertedIndex[size] = qIndex;
            size++;
            bubleUp(size - 1);
        }

        void extract(int k) {
            size--;
            double d = heap[k];
            if (size > k) {
                // Move the last element to the k-th position
                heap[k] = heap[size];
                invertedIndex[k] = invertedIndex[size];
                window[invertedIndex[k]] = k;
                windowHeap[invertedIndex[k]] = this;
            }
            bubleUp(k);
            bubleDown(k);
        }

        void transfer(int k, Heap other) {
            other.insert(heap[k], invertedIndex[k]);
            extract(k);
        }

        void exchange(int k, Heap other, int m) {
            double val = other.heap[m];
            int qIndex = other.invertedIndex[m];
            other.extract(m);
            transfer(k, other);
            insert(val, qIndex);
        }

        private void swap(int i, int j) {
            int eli = invertedIndex[i];
            int elj = invertedIndex[j];
            double di = heap[i];
            double dj = heap[j];
            window[elj] = i;
            window[eli] = j;
            heap[j] = di;
            heap[i] = dj;
            invertedIndex[i] = elj;
            invertedIndex[j] = eli;
        }

        private void bubleUp(int k) {
            while (k > 0) {
                int parent = parentOf(k);
                boolean swap = minHeap ? heap[parent] > heap[k] : heap[parent] < heap[k];
                if (swap) {
                    swap(k, parent);
                }
                k = parent;
            }
        }

        private void bubleDown(int k) {
            int half = size >>> 1;
            while (k < half) {
                double val = heap[k];
                int leftChild = leftChildOf(k);
                int rightChild = leftChild + 1;
                int child = leftChild;
                double childVal = heap[child];
                boolean swap;

                if (rightChild < size && (minHeap ? heap[rightChild] < childVal : heap[rightChild] > childVal)) {
                    child = rightChild;
                    childVal = heap[child];
                }
                swap = minHeap ? childVal < val : childVal > val;
                if (!swap)
                    break;
                swap(k, child);
                k = child;
            }
        }

        private int parentOf(int k) {
            return (k - 1) >>> 1;
        }

        private int leftChildOf(int k) {
            return 2 * k + 1;
        }

        private int rightChildOf(int i) {
            return leftChildOf(i) + 1;
        }
    }
}
