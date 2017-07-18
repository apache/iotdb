package cn.edu.thu.tsfiledb.qp.executor.iterator;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import cn.edu.thu.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.support.Field;
import cn.edu.thu.tsfile.timeseries.read.support.RowRecord;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.physical.crud.SeriesSelectPlan;

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
    private Node[] nodes;
    // it's actually number of series iterators which has next record;
    private int heapSize;
    private long lastRowTime = -1;

    public MergeQuerySetIterator(List<SeriesSelectPlan> selectPlans, int mergeFetchSize,
                                 QueryProcessExecutor executor) throws QueryProcessorException {
        this.mergeFetchSize = mergeFetchSize;
        heapSize = selectPlans.size();
        nodes = new Node[heapSize + 1];
        recordIters = SeriesSelectPlan.getRecordIteratorArray(selectPlans, executor);
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
                nodes[index++] = new Node(recordIters[i].next(), recordIters[i]);
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
            Node minNode = nodes[1];
            if (minNode.rowRecord.timestamp != lastRowTime) {
                lastRowTime = minNode.rowRecord.timestamp;
                i++;
                // ret.putARowRecord(minNode.r);
                addNewRecordToQueryDataSet(ret, minNode.rowRecord);
            }
            if (minNode.iter.hasNext()) {
                nodes[1].rowRecord = nodes[1].iter.next();
            } else {
                nodes[1] = nodes[heapSize];
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
        if (left <= heapSize && nodes[left].lessThan(nodes[i]))
            min = left;
        if (right <= heapSize && nodes[right].lessThan(nodes[min]))
            min = right;

        if (min != i) {
            Node tmp = nodes[i];
            nodes[i] = nodes[min];
            nodes[min] = tmp;
            minHeapify(min);
        }
    }

    private class Node {
        public RowRecord rowRecord;
        public Iterator<RowRecord> iter;

        public Node(RowRecord rowRecord, Iterator<RowRecord> iter) {
            this.rowRecord = rowRecord;
            this.iter = iter;
        }

        public boolean lessThan(Node o) {
            return rowRecord.timestamp <= o.rowRecord.timestamp;
        }

        @Override
        public String toString() {
            return rowRecord.toString();
        }
    }

    private void addNewRecordToQueryDataSet(QueryDataSet dataSet, RowRecord record) {
        for (Field f : record.fields) {
            StringBuilder sb = new StringBuilder();
            sb.append(f.deltaObjectId);
            sb.append(".");
            sb.append(f.measurementId);
            String key = sb.toString();

            LinkedHashMap<String, DynamicOneColumnData> mapRet = dataSet.mapRet;
            if (!mapRet.containsKey(key)) {
                DynamicOneColumnData oneCol = new DynamicOneColumnData(f.dataType, true);
                oneCol.setDeltaObjectType(record.deltaObjectType);
                mapRet.put(key, oneCol);
            }

            // only when f is not null, the value of f could be put into DynamicOneColumnData.
            switch (f.dataType) {
                case BOOLEAN:
                    if (!f.isNull()) {
                        mapRet.get(key).putTime(record.timestamp);
                        mapRet.get(key).putBoolean(f.getBoolV());
                    }
                    break;
                case INT32:
                    if (!f.isNull()) {
                        mapRet.get(key).putTime(record.timestamp);
                        mapRet.get(key).putInt(f.getIntV());
                    }
                    break;
                case INT64:
                    if (!f.isNull()) {
                        mapRet.get(key).putTime(record.timestamp);
                        mapRet.get(key).putLong(f.getLongV());
                    }
                    break;
                case FLOAT:
                    if (!f.isNull()) {
                        mapRet.get(key).putTime(record.timestamp);
                        mapRet.get(key).putFloat(f.getFloatV());
                    }
                    break;
                case DOUBLE:
                    if (!f.isNull()) {
                        mapRet.get(key).putTime(record.timestamp);
                        mapRet.get(key).putDouble(f.getFloatV());
                    }
                    break;
                case BYTE_ARRAY:
                    if (!f.isNull()) {
                        mapRet.get(key).putTime(record.timestamp);
                        mapRet.get(key).putBinary(f.getBinaryV());
                    }
                    break;
                case ENUMS:
                    if (!f.isNull()) {
                        mapRet.get(key).putTime(record.timestamp);
                        mapRet.get(key).putBinary(f.getBinaryV());
                    }
                    break;
                default:
                    throw new UnSupportedDataTypeException("UnSupported" + String.valueOf(f.dataType));
            }
            // mapRet.get(key).putTime(record.timestamp);
        }
    }
}
