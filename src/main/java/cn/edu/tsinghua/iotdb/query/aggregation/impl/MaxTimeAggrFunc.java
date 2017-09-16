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

public class MaxTimeAggrFunc extends AggregateFunction {

    private boolean hasSetValue = false;

    public MaxTimeAggrFunc() {
        super(AggregationConstant.MAX_TIME, TSDataType.INT64);
        result.data.putTime(0);
    }

    @Override
    public void calculateValueFromPageHeader(PageHeader pageHeader) {
        long timestamp = pageHeader.data_page_header.max_timestamp;
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
        long timestamp = dataInThisPage.getTime(dataInThisPage.valueLength - 1);
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
    public int calculateValueFromDataPage(DynamicOneColumnData dataInThisPage, List<Long> timestamps, int timeIndex) {
        return 0;
    }

    @Override
    public void calculateValueFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        Object max_time = insertMemoryData.calcAggregation(AggregationConstant.MAX_TIME);
        if (max_time != null) {
            long timestamp = (long) max_time;
            if (!hasSetValue) {
                result.data.putLong(timestamp);
                hasSetValue = true;
            } else {
                long maxv = result.data.getLong(0);
                maxv = maxv > timestamp ? maxv : timestamp;
                result.data.setLong(0, maxv);
            }
        }
    }

    @Override
    public boolean calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException, ProcessorException {
        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    if (!hasSetValue) {
                        result.data.putLong(timestamps.get(timeIndex));
                        hasSetValue = true;
                    } else {
                        long maxv = result.data.getLong(0);
                        maxv = maxv > timestamps.get(timeIndex) ? maxv : timestamps.get(timeIndex);
                        result.data.setLong(0, maxv);
                    }
                    timeIndex ++;
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

}
