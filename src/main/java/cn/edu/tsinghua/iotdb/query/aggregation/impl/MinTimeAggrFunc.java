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

public class MinTimeAggrFunc extends AggregateFunction {

    private boolean hasSetValue = false;

    public MinTimeAggrFunc() {
        super(AggregationConstant.MIN_TIME, TSDataType.INT64);
    }

    @Override
    public void calculateValueFromPageHeader(PageHeader pageHeader) {
        if (result.data.timeLength == 0) {
            result.data.putTime(0);
        }

        long timestamp = pageHeader.data_page_header.min_timestamp;
        updateMinTime(timestamp);
    }

    @Override
    public void calculateValueFromDataPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        if (result.data.timeLength == 0) {
            result.data.putTime(0);
        }

        if (dataInThisPage.valueLength == 0) {
            return;
        }
        // use the first timestamp of the DynamicOneColumnData
        long timestamp = dataInThisPage.getTime(0);
        updateMinTime(timestamp);
    }

    @Override
    public int calculateValueFromDataPage(DynamicOneColumnData dataInThisPage, List<Long> timestamps, int timeIndex) {
        return 0;
    }

    @Override
    public void calculateValueFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        if (result.data.timeLength == 0) {
            result.data.putTime(0);
        }

        Object min_time = insertMemoryData.calcAggregation(AggregationConstant.MIN_TIME);
        if (min_time != null) {
            long timestamp = (long) min_time;
            updateMinTime(timestamp);
        }
    }

    @Override
    public boolean calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException, ProcessorException {
        if (result.data.timeLength == 0) {
            result.data.putTime(0);
        }

        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    updateMinTime(timestamps.get(timeIndex));
                    return false;
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
    public void calcGroupByAggregationWithoutFilter(long partitionStart, long intervalStart, long intervalEnd, DynamicOneColumnData data) {

    }

    private void updateMinTime(long timestamp) {
        if (!hasSetValue) {
            result.data.putLong(timestamp);
            hasSetValue = true;
        } else {
            long mint = result.data.getLong(0);
            mint = mint < timestamp ? mint : timestamp;
            result.data.setLong(0, mint);
        }
    }
}
