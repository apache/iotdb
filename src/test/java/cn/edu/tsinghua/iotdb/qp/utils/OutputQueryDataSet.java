package cn.edu.tsinghua.iotdb.qp.utils;

import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;

/**
 * This getIndex data set is used for getIndex processing. getIndex processing merges a list of QueryDataSet
 * and construct a new OutputQueryDataSet by adding {@code RowRecord}. This class provides two
 * methods for caller {@code getNextRecord} and {@code hasNextRecord} just same as
 * {@code QueryDataSet}.
 * 
 * @author kangrong
 *
 */
public class OutputQueryDataSet extends QueryDataSet {
    protected final int fetchSize;
    protected RowRecord[] data;
    protected int size;
    protected int index;

    public OutputQueryDataSet(int fetchSize) {
        this.fetchSize = fetchSize;
        data = new RowRecord[fetchSize];
        size = 0;
        index = 0;
    }

    /**
     * put a rowRecord into this DataSet.
     * 
     * @param r - rowRecord to be added.
     * @return if amount of exist record equals to fetchSize, return false, otherwise return true.
     * 
     */
    public boolean addRowRecord(RowRecord r) {
        if (size < fetchSize) {
            data[size++] = r;
            return true;
        } else
            return false;
    }

    /**
     * For efficiency, this method don't check index boundary. caller should check index boundary by
     * {@code hasNextRecord} before calling this method.
     */
    @Override
    public RowRecord getNextRecord() {
        return data[index++];
    }

    @Override
    public boolean hasNextRecord() {
        return index < size;
    }
}
