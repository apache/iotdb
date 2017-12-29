package cn.edu.tsinghua.iotdb.query.aggregation.impl;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.IOException;
import java.util.List;

public class SumAggrFunc extends AggregateFunction {

    private double sum;
    private boolean hasValue = false;

    public SumAggrFunc() {
        super(AggregationConstant.SUM, TSDataType.DOUBLE);
    }

    @Override
    public void putDefaultValue() {
        resultData.putEmptyTime(0);
    }

    @Override
    public void calculateValueFromPageHeader(PageHeader pageHeader) throws ProcessorException {
        if(resultData.timeLength == 0)
            resultData.putTime(0);
        String sumValStr = pageHeader.data_page_header.digest.getStatistics().get(AggregationConstant.SUM);
        if(sumValStr == null)
            throw new ProcessorException("PageHeader contains no SUM value");
        double pageSum;
        try {
            pageSum = Double.parseDouble(sumValStr);
        } catch (NumberFormatException e) {
            throw new ProcessorException("Sum in page header is not a double! "  + e.getMessage());
        }
        if(pageHeader.data_page_header.num_rows > 0)
            hasValue = true;

        sum += pageSum;
        updateSum();
    }

    @Override
    public void calculateValueFromDataPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        // TODO : update mean or update sum?
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }
        updateSum(dataInThisPage);
    }

    @Override
    public int calculateValueFromDataPage(DynamicOneColumnData dataInThisPage, List<Long> timestamps, int timeIndex) {
        return 0;
    }

    @Override
    public void calculateValueFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }
        updateSum(insertMemoryData);
    }

    @Override
    public boolean calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }
        // use switch here to reduce switch usage to once
        switch (insertMemoryData.getDataType()) {
            case INT32:
                updateSumWithInt(insertMemoryData, timestamps, timeIndex);
                break;
            case INT64:
                updateSumWithLong(insertMemoryData, timestamps, timeIndex);
                break;
            case FLOAT:
                updateSumWithFloat(insertMemoryData, timestamps, timeIndex);
                break;
            case DOUBLE:
                updateSumWithDouble(insertMemoryData, timestamps, timeIndex);
                break;
            case INT96:
            case TEXT:
            case ENUMS:
            case BOOLEAN:
            case FIXED_LEN_BYTE_ARRAY:
            case BIGDECIMAL:
            default:
                throw new ProcessorException("Unsupported data type in aggregation MEAN : " + insertMemoryData.getDataType());
        }

        return insertMemoryData.hasInsertData();
    }

    @Override
    public void calcGroupByAggregation(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd, DynamicOneColumnData data) throws ProcessorException {
        if (resultData.emptyTimeLength == 0) {
            if (resultData.timeLength == 0) {
                resultData.putEmptyTime(partitionStart);
                reset();
            } else if (resultData.getTime(resultData.timeLength - 1) != partitionStart) {
                resultData.putEmptyTime(partitionStart);
                reset();
            }
        } else {
            if ((resultData.getEmptyTime(resultData.emptyTimeLength - 1) != partitionStart)
                    && (resultData.timeLength == 0 ||
                    (resultData.timeLength > 0 && resultData.getTime(resultData.timeLength - 1) != partitionStart))) {
                resultData.putEmptyTime(partitionStart);
                reset();
            }
        }

        // use switch here to reduce switch usage to once
        switch (data.dataType) {
            case INT32:
                groupUpdateMeanWithInt(partitionStart, partitionEnd, intervalStart, intervalEnd, data);
                break;
            case INT64:
                groupUpdateMeanWithLong(partitionStart, partitionEnd, intervalStart, intervalEnd, data);
                break;
            case FLOAT:
                groupUpdateMeanWithFloat(partitionStart, partitionEnd, intervalStart, intervalEnd, data);
                break;
            case DOUBLE:
                groupUpdateMeanWithDouble(partitionStart, partitionEnd, intervalStart, intervalEnd, data);
                break;
            case INT96:
            case TEXT:
            case ENUMS:
            case BOOLEAN:
            case FIXED_LEN_BYTE_ARRAY:
            case BIGDECIMAL:
            default:
                throw new ProcessorException("Unsupported data type in aggregation MEAN : " + data.dataType);
        }
    }

    /**
     *  Update sum use all data in a column.
     * @param data
     * @throws ProcessorException
     */
    private void updateSum(DynamicOneColumnData data) throws ProcessorException {
        switch (data.dataType) {
            case INT32:
                for(; data.curIdx < data.timeLength; data.curIdx++) {
                    sum += data.getInt(data.curIdx);
                    hasValue = true;
                }
            case INT64:
                for(; data.curIdx < data.timeLength; data.curIdx++) {
                    sum += data.getLong(data.curIdx);
                    hasValue = true;
                }
                break;
            case FLOAT:
                for(; data.curIdx < data.timeLength; data.curIdx++) {
                    sum += data.getFloat(data.curIdx);
                    hasValue = true;
                }
                break;
            case DOUBLE:
                for(; data.curIdx < data.timeLength; data.curIdx++) {
                    sum += data.getDouble(data.curIdx);
                    hasValue = true;
                }
                break;
            case INT96:
            case TEXT:
            case ENUMS:
            case BOOLEAN:
            case FIXED_LEN_BYTE_ARRAY:
            case BIGDECIMAL:
            default:
                throw new ProcessorException("Unsupported data type in aggregation MEAN : " + data.dataType);
        }
        updateSum();
    }

    private void updateSum(InsertDynamicData data) throws ProcessorException {
        switch (data.getDataType()) {
            case INT32:
                try {
                    while (data.hasInsertData()) {
                        sum += data.getCurrentIntValue();
                        data.removeCurrentValue();
                        hasValue = true;
                    }
                } catch (IOException e) {
                    throw new ProcessorException(e.getMessage());
                }
                break;
            case INT64:
                try {
                    while (data.hasInsertData()) {
                        sum += data.getCurrentLongValue();
                        data.removeCurrentValue();
                        hasValue = true;
                    }
                } catch (IOException e) {
                    throw new ProcessorException(e.getMessage());
                }
                break;
            case FLOAT:
                try {
                    while (data.hasInsertData()) {
                        sum += data.getCurrentFloatValue();
                        data.removeCurrentValue();
                        hasValue = true;
                    }
                } catch (IOException e) {
                    throw new ProcessorException(e.getMessage());
                }
                break;
            case DOUBLE:
                try {
                    while (data.hasInsertData()) {
                        sum += data.getCurrentDoubleValue();
                        data.removeCurrentValue();
                        hasValue = true;
                    }
                } catch (IOException e) {
                    throw new ProcessorException(e.getMessage());
                }
                break;
            case INT96:
            case TEXT:
            case ENUMS:
            case BOOLEAN:
            case FIXED_LEN_BYTE_ARRAY:
            case BIGDECIMAL:
            default:
                throw new ProcessorException("Unsupported data type in aggregation MEAN : " + data.getDataType());
        }
        updateSum();
    }

    // TODO : use function template generator
    /**
     * Update mean using values in a column whose timestamp is contained in timestamps[timeIndex:].
     * Use different method to avoid redundant switches.
     * @param insertMemoryData
     * @param timestamps
     * @param timeIndex
     * @throws IOException
     */
    private void updateSumWithInt(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException {
        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    //System.out.println(">>>> dy >> " + insertMemoryData.getCurrentMinTime() + " " + insertMemoryData.getCurrentIntValue());
                    int val = insertMemoryData.getCurrentIntValue();
                    sum += val;
                    timeIndex ++;
                    insertMemoryData.removeCurrentValue();
                    hasValue = true;
                } else if (timestamps.get(timeIndex) > insertMemoryData.getCurrentMinTime()) {
                    insertMemoryData.removeCurrentValue();
                } else {
                    timeIndex += 1;
                }
            } else {
                break;
            }
        }
        updateSum();
    }

    private void updateSumWithLong(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException {
        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    Long val = insertMemoryData.getCurrentLongValue();
                    sum += val;
                    timeIndex ++;
                    insertMemoryData.removeCurrentValue();
                    hasValue = true;
                } else if (timestamps.get(timeIndex) > insertMemoryData.getCurrentMinTime()) {
                    insertMemoryData.removeCurrentValue();
                } else {
                    timeIndex += 1;
                }
            } else {
                break;
            }
        }
        updateSum();
    }

    private void updateSumWithFloat(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException {
        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    float val = insertMemoryData.getCurrentFloatValue();
                    sum += val;
                    timeIndex ++;
                    insertMemoryData.removeCurrentValue();
                    hasValue = true;
                } else if (timestamps.get(timeIndex) > insertMemoryData.getCurrentMinTime()) {
                    insertMemoryData.removeCurrentValue();
                } else {
                    timeIndex += 1;
                }
            } else {
                break;
            }
        }
        updateSum();
    }

    private void updateSumWithDouble(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException {
        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    double val = insertMemoryData.getCurrentDoubleValue();
                    sum += val;
                    timeIndex ++;
                    insertMemoryData.removeCurrentValue();
                    hasValue = true;
                } else if (timestamps.get(timeIndex) > insertMemoryData.getCurrentMinTime()) {
                    insertMemoryData.removeCurrentValue();
                } else {
                    timeIndex += 1;
                }
            } else {
                break;
            }
        }
        updateSum();
    }

    /**
     *  Update mean using data that fall in given window, the window is the intersection of [partitionStart, partitionEnd]
     *  and [intervalStart, intervalEnd].
     * @param partitionStart
     * @param partitionEnd
     * @param intervalStart
     * @param intervalEnd
     * @param data
     */
    private void groupUpdateMeanWithInt(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd, DynamicOneColumnData data) {
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                int val = data.getInt(data.curIdx);
                sum += val;
                data.curIdx++;
                hasValue = true;
            }
        }
        groupUpdateMean(partitionStart);
    }

    private void groupUpdateMeanWithLong(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd, DynamicOneColumnData data) {
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                long val = data.getLong(data.curIdx);
                sum += val;
                data.curIdx++;
                hasValue = true;
            }
        }
        groupUpdateMean(partitionStart);
    }

    private void groupUpdateMeanWithFloat(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd, DynamicOneColumnData data) {
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                float val = data.getFloat(data.curIdx);
                sum += val;
                data.curIdx++;
                hasValue = true;
            }
        }
        groupUpdateMean(partitionStart);
    }

    private void groupUpdateMeanWithDouble(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd, DynamicOneColumnData data) {
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                double val = data.getDouble(data.curIdx);
                sum += val;
                data.curIdx++;
                hasValue = true;
            }
        }
        groupUpdateMean(partitionStart);
    }

    private void updateSum() {
        if(hasValue) {
            if(resultData.valueLength == 0)
                resultData.putDouble(sum);
            else
                resultData.setDouble(0, sum);
        }
    }

    private void groupUpdateMean(long partitionStart) {
        if(hasValue) {
            if (resultData.emptyTimeLength > 0 && resultData.getEmptyTime(resultData.emptyTimeLength - 1) == partitionStart) {
                resultData.removeLastEmptyTime();
                resultData.putTime(partitionStart);
                resultData.putDouble(sum);
            } else {
                resultData.setDouble(resultData.valueLength - 1, sum);
            }
        }
    }

    private void reset() {
        sum = 0.0;
        hasValue = false;
    }
}
