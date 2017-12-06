package cn.edu.tsinghua.iotdb.query.aggregation.impl;

import java.io.IOException;
import java.util.List;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

public class CountAggrFunc extends AggregateFunction {

    public CountAggrFunc() {
        super(AggregationConstant.COUNT, TSDataType.INT64);
    }

    @Override
    public void putDefaultValue() {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
            resultData.putLong(0);
        }
    }

    @Override
    public void calculateValueFromPageHeader(PageHeader pageHeader) {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
            resultData.putLong(0);
        }

        long preValue = resultData.getLong(0);
        preValue += pageHeader.data_page_header.num_rows;
        resultData.setLong(0, preValue);

    }

    @Override
    public void calculateValueFromDataPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
            resultData.putLong(0);
        }

        long preValue = resultData.getLong(0);
        preValue += dataInThisPage.valueLength;
        resultData.setLong(0, preValue);
    }

    @Override
    public int calculateValueFromDataPage(DynamicOneColumnData dataInThisPage, List<Long> timestamps, int timeIndex) {
        return 0;
    }

    @Override
    public void calculateValueFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
            resultData.putLong(0);
        }

        long preValue = resultData.getLong(0);
        Object count = insertMemoryData.calcAggregation(AggregationConstant.COUNT);
        preValue += (long) count;
        resultData.setLong(0, preValue);
    }

    @Override
    public boolean calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex)
            throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
            resultData.putLong(0);
        }

        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (insertMemoryData.getCurrentMinTime() >= 2495) {
                    //System.out.println("...");
                }
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    long preValue = resultData.getLong(0);
                    preValue += 1;
                    resultData.setLong(0, preValue);
                    timeIndex++;
                    insertMemoryData.removeCurrentValue();
                } else if (timestamps.get(timeIndex) > insertMemoryData.getCurrentMinTime()) {
                    insertMemoryData.removeCurrentValue();
                } else {
                    timeIndex += 1;
                }
            } else {
                break;
            }
        }

        return insertMemoryData.hasInsertData();
    }

    @Override
    public void calcGroupByAggregation(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd,
                                       DynamicOneColumnData data) {

        if (resultData.emptyTimeLength == 0) {
            if (resultData.timeLength == 0) {
                resultData.putEmptyTime(partitionStart);
            } else if (resultData.getTime(resultData.timeLength - 1) != partitionStart) {
                resultData.putEmptyTime(partitionStart);
            }
        } else {
            if ((resultData.getEmptyTime(resultData.emptyTimeLength - 1) != partitionStart)
                    && (resultData.timeLength == 0 ||
                    (resultData.timeLength > 0 && resultData.getTime(resultData.timeLength - 1) != partitionStart)))
                resultData.putEmptyTime(partitionStart);
        }

        long valueSum = 0;
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                valueSum++;
                data.curIdx++;
            }
        }

        if (valueSum > 0) {
            if (resultData.emptyTimeLength > 0 && resultData.getEmptyTime(resultData.emptyTimeLength - 1) == partitionStart) {
                resultData.removeLastEmptyTime();
                resultData.putTime(partitionStart);
                resultData.putLong(valueSum);
            } else {
                long preSum = resultData.getLong(resultData.valueLength - 1);
                resultData.setLong(resultData.valueLength - 1, preSum + valueSum);
            }
        }
    }
}
