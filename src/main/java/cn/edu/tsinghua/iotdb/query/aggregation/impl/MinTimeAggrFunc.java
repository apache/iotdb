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
        result.data.putTime(0);
    }

    @Override
    public void calculateValueFromPageHeader(PageHeader pageHeader) {
        long timestamp = pageHeader.data_page_header.min_timestamp;
        if (!hasSetValue) {
            result.data.putLong(timestamp);
            hasSetValue = true;
        } else {
            long maxv = result.data.getLong(0);
            maxv = maxv > timestamp ? maxv : timestamp;
            result.data.setLong(0, maxv);
        }
    }

    @Override
    public void calculateValueFromDataPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        if (dataInThisPage.valueLength == 0) {
            return;
        }
        // use the first timestamp of the DynamicOneColumnData
        long timestamp = dataInThisPage.getTime(0);
        if (!hasSetValue) {
            result.data.putLong(timestamp);
            hasSetValue = true;
        } else {
            long minv = result.data.getLong(0);
            minv = minv < timestamp ? minv : timestamp;
            result.data.setLong(0, minv);
        }
    }

    @Override
    public int calculateValueFromDataPage(DynamicOneColumnData dataInThisPage, List<Long> timestamps, int timeIndex) {
        return 0;
    }

    @Override
    public void calculateValueFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        Object min_time = insertMemoryData.calcAggregation(AggregationConstant.MIN_TIME);
        if (min_time != null) {
            long timestamp = (long) min_time;
            if (!hasSetValue) {
                result.data.putLong(timestamp);
                hasSetValue = true;
            } else {
                long minv = result.data.getLong(0);
                minv = minv < timestamp ? minv : timestamp;
                result.data.setLong(0, minv);
            }
        }
    }

    @Override
    public boolean calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException, ProcessorException {
        return false;
    }

}
