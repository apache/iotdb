package cn.edu.tsinghua.iotdb.query.aggregation;

import cn.edu.tsinghua.iotdb.query.reader.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.IOException;
import java.util.List;

public abstract class AggregateFunction {

    public String name;
    public DynamicOneColumnData resultData;
    public TSDataType dataType;
    public boolean hasSetValue;

    public AggregateFunction(String name, TSDataType dataType) {
        this.name = name;
        this.dataType = dataType;
        resultData = new DynamicOneColumnData(dataType, true, true);
    }

    public abstract void putDefaultValue();

    /**
     * <p>
     * Calculate the aggregation using <code>PageHeader</code>.
     * </p>
     *
     * @param pageHeader <code>PageHeader</code>
     */
    public abstract void calculateValueFromPageHeader(PageHeader pageHeader) throws ProcessorException;

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

    /**
     * <p>
     * This method is calculate the group by function.
     * </p>
     *
     * @param partitionStart
     * @param partitionEnd
     * @param intervalStart
     * @param intervalEnd
     * @param data
     */
    public abstract void calcGroupByAggregation(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd,
                                                DynamicOneColumnData data) throws ProcessorException;

    /**
     * Convert a value from string to its real data type and put into return data.
     * @param valueStr
     */
    public void putValueFromStr(String valueStr) throws ProcessorException {
        try{
            switch (dataType) {
                case INT32:
                    resultData.putInt(Integer.parseInt(valueStr));
                    break;
                case INT64:
                    resultData.putLong(Long.parseLong(valueStr));
                    break;
                case BOOLEAN:
                    resultData.putBoolean(Boolean.parseBoolean(valueStr));
                    break;
                case ENUMS:
                case TEXT:
                    resultData.putBinary(new Binary(valueStr));
                    break;
                case DOUBLE:
                    resultData.putDouble(Double.parseDouble(valueStr));
                    break;
                case FLOAT:
                    resultData.putFloat(Float.parseFloat(valueStr));
                    break;
                default:
                    throw new ProcessorException("Unsupported type " + dataType);
            }
        } catch (Exception e) {
          throw new ProcessorException(e.getMessage());
        }
    }
}
