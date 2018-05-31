package cn.edu.tsinghua.iotdb.qp.utils;

import cn.edu.tsinghua.tsfile.timeseries.read.query.OnePassQueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.OldRowRecord;

/**
 * This getIndex data set is used for getIndex processing. getIndex processing merges a list of OnePassQueryDataSet
 * and construct a new OutputOnePassQueryDataSet by adding {@code RowRecord}. This class provides two
 * methods for caller {@code getNextRecord} and {@code hasNextRecord} just same as
 * {@code OnePassQueryDataSet}.
 * 
 * @author kangrong
 *
 */
public class OutputOnePassQueryDataSet extends OnePassQueryDataSet {
    protected final int fetchSize;
    protected OldRowRecord[] data;
    protected int size;
    protected int index;

    public OutputOnePassQueryDataSet(int fetchSize) {
        this.fetchSize = fetchSize;
        data = new OldRowRecord[fetchSize];
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
    public boolean addRowRecord(OldRowRecord r) {
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
    public OldRowRecord getNextRecord() {
        return data[index++];
    }

    @Override
    public boolean hasNextRecord() {
        return index < size;
    }
}
