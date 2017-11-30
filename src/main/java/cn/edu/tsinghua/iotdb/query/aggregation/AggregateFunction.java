package cn.edu.tsinghua.iotdb.query.aggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

public abstract class AggregateFunction {

    public String name;
    public AggregationResult result;
    public TSDataType dataType;
    /**
     * storage some necessary object for batch read, such as incompletely read Page
     */
    public Map<String, Object> maps = new HashMap<>();

    public AggregateFunction(String name, TSDataType dataType) {
        this.name = name;
        this.dataType = dataType;
        result = new AggregationResult(dataType);
    }

    public AggregateFunction(String name, TSDataType dataType, boolean hasEmptyTime) {
        this.name = name;
        this.dataType = dataType;
        result = new AggregationResult(dataType, true);
    }

    public abstract void putDefaultValue();

    /**
     * <p>
     * Calculate the aggregation using <code>PageHeader</code>.
     * </p>
     *
     * @param pageHeader <code>PageHeader</code>
     */
    public abstract void calculateValueFromPageHeader(PageHeader pageHeader);

    /**
     * <p>
     * Could not calculate using <method>calculateValueFromPageHeader</method> directly.
     * Calculate the aggregation according to all decompressed data in this page.
     * </p>
     *
     * @param dataInThisPage the data in the DataPage
     * @throws IOException        TsFile data read exception
     * @throws ProcessorException wrong aggregation method parameter
     */
    public abstract void calculateValueFromDataPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException;

    /**
     * <p>
     * Calculate the aggregation using <code>PageHeader</code> along with given timestamps.
     * </p>
     *
     * @param dataInThisPage Page data after overflow/bufferwrite operation
     * @param timestamps     given timestamps, must consider in aggregation calculate
     * @param timeIndex      represents the read index of timestamps
     * @return the index of read of timestamps after executing this method
     */
    public abstract int calculateValueFromDataPage(DynamicOneColumnData dataInThisPage, List<Long> timestamps, int timeIndex);

    /**
     * <p>
     * Calculate the aggregation in <code>InsertDynamicData</code>.
     * </p>
     *
     * @param insertMemoryData the data in the DataPage with bufferwrite and overflow data
     * @throws IOException        TsFile data read exception
     * @throws ProcessorException wrong aggregation method parameter
     */
    public abstract void calculateValueFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException;

    /**
     * <p>
     * This method is calculate the aggregation using the common timestamps of cross series filter.
     * </p>
     *
     * @param insertMemoryData the data in memory which contains bufferwrite along with overflow operation
     * @param timestamps the common timestamps given which must be considered
     * @param timeIndex the used index of timestamps
     * @throws IOException        TsFile data read error
     * @throws ProcessorException wrong aggregation method parameter
     */
    public abstract boolean calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex)
            throws IOException, ProcessorException;


    public abstract void calcGroupByAggregation(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd,
                                                DynamicOneColumnData data);
}
