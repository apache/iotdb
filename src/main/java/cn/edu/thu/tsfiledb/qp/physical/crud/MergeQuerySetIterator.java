package cn.edu.thu.tsfiledb.qp.physical.crud;

import java.util.Iterator;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.readSupport.RowRecord;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;

/**
 * This class implements the interface {@code Iterator<QueryDataSet>}. It is the result of
 * {@code MergeQuerySetPlan}(for multi-pass getIndex). {@code MergeQuerySetPlan} provides it with a
 * list of {@code SeriesSelectPlan}.<br>
 * This class merge row record data set from a list of {@code Iterator<RowRecord>} provided by
 * {@code SeriesSelectPlan} according to the time ascending, using <em>minimum heap</em>
 * 
 * @author kangrong
 *
 */
public class MergeQuerySetIterator implements Iterator<QueryDataSet> {
    private final int mergeFetchSize;
    private Iterator<RowRecord>[] recordIters;
    private Node[] records;
    // it's actually number of series iterators which has next record;
    private int heapSize;
    private long lastRowTime = -1;

    public MergeQuerySetIterator(List<SeriesSelectPlan> selectPlans, int mergeFetchSize,
                                 QueryProcessExecutor conf) throws QueryProcessorException {
        this.mergeFetchSize = mergeFetchSize;
        heapSize = selectPlans.size();
        records = new Node[heapSize + 1];
        recordIters = SeriesSelectPlan.getRecordIteratorArray(selectPlans, conf);
        initIters();
    }

    private void initIters() {
        int index = 1;
        int tempSize = heapSize;
        for (int i = 0; i < tempSize; i++) {
            if (!recordIters[i].hasNext()) {
                heapSize--;
            } else {
                // add first value in all iterators to build minimum heap.
                records[index++] = new Node(recordIters[i].next(), recordIters[i]);
            }
        }
        // build minimum Heap
        for (int i = heapSize / 2; i >= 1; i--)
            minHeapify(i);
    }

    @Override
    public boolean hasNext() {
        return heapSize > 0;
    }

    @Override
    public QueryDataSet next() {
        QueryDataSet ret = new QueryDataSet();
        int i = 0;
        while (i < mergeFetchSize && heapSize > 0) {
            Node minNode = records[1];
            if (minNode.r.timestamp != lastRowTime) {
                lastRowTime = minNode.r.timestamp;
                i++;
                ret.putARowRecord(minNode.r);
            }
            if (minNode.iter.hasNext()) {
                records[1].r = records[1].iter.next();
            } else {
                records[1] = records[heapSize];
                heapSize -= 1;
            }
            minHeapify(1);
        }
        return ret;
    }

    public void minHeapify(int i) {
        int left = 2 * i;
        int right = 2 * i + 1;
        int min = i;

        if (left <= heapSize && records[left].lessThan(records[i]))
            min = left;
        if (right <= heapSize && records[right].lessThan(records[min]))
            min = right;

        if (min != i) {
            Node tmp = records[i];
            records[i] = records[min];
            records[min] = tmp;
            minHeapify(min);
        }
    }

    private class Node {
        public RowRecord r;
        public Iterator<RowRecord> iter;

        public Node(RowRecord r, Iterator<RowRecord> iter) {
            this.r = r;
            this.iter = iter;
        }

        public boolean lessThan(Node o) {
            return r.timestamp <= o.r.timestamp;
        }

        @Override
        public String toString() {
            return r.toString();
        }
    }
}
