package cn.edu.tsinghua.iotdb.engine.overflow.treeV2;

import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperation;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * we advise the implementation class of this interface can be constructed by two ways:<br>
 * 1.construct a empty index without parameters. 2.construct a empty index with a input stream for
 * restoring.
 *
 * @author kangrong
 */
public interface IIntervalTreeOperator {
    /**
     * insert a value to a time point. Insert doesn't support time range insert
     *
     * @param t     - time
     * @param value - value
     */
    void insert(long t, byte[] value);

    /**
     * update a value to a time point or a time range.
     *
     * @param s     - start time.
     * @param e     - end time.
     * @param value - value to be updated.
     */
    void update(long s, long e, byte[] value);

    /**
     * The implementation maintains an overflow index in memory. The data in the index is prior to
     * <em>newerMemoryData</em>, which means the overflow operators corresponding to the index are covered with
     * <em>newerMemoryData</em>. This function merges current index into <em>newerMemoryData</em> and return the
     * merged result.
     *
     * @param timeFilter      - timeFilter is specified by user.
     * @param valueFilter     - valueFilter is specified by user.
     * @param newerMemoryData - newer overflow data.
     * @return merged result.
     */
    DynamicOneColumnData queryMemory(SingleSeriesFilterExpression timeFilter,
                                     SingleSeriesFilterExpression valueFilter, DynamicOneColumnData newerMemoryData);

    /**
     * This function merges the older data which deserialized from given parameter <em>in</em> into <em>newerData</em>
     * and return the merged result. The data in <em>in</em> is prior to <em>newerData</em>, which means the overflow
     * operators corresponding to <em>in</em> are covered with <em>newerData</em>.
     *
     * @param timeFilter  - timeFilter is specified by user.
     * @param valueFilter - valueFilter is specified by user.
     * @param in          - the inputstream to be merged into newerData which contains older overflow data .
     * @param newerData   - newer overflow data.
     * @return merged result.
     */
    DynamicOneColumnData queryFileBlock(SingleSeriesFilterExpression timeFilter,
                                        SingleSeriesFilterExpression valueFilter, InputStream in,
                                        DynamicOneColumnData newerData) throws IOException;

    /**
     * Get List<OverflowOperation>(insert operations, update operations and delete operations which meet the expression of time filter,
     * value filter and frequency filter in DynamicOneColumnData data.)
     *
     * @param data        - a DynamicOneColumnData information.
     * @return - List<OverflowOperation>
     */
    List<OverflowOperation> getDynamicList(DynamicOneColumnData data);

    /**
     * delete all values earlier than timestamp.
     *
     * @param timestamp - delete timestamp
     */
    void delete(long timestamp);

    /**
     * given an outputstream, serialize the index into it.
     *
     * @param out - serialization output stream.
     */
    void toBytes(OutputStream out) throws IOException;

    /**
     * @return the memory size for this index
     */
    long calcMemSize();

    boolean isEmpty();
}
