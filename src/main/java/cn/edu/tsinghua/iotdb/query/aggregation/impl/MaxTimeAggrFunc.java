package cn.edu.tsinghua.iotdb.query.aggregation.impl;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.iotdb.query.reader.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

import java.io.IOException;
import java.util.List;

public class MaxTimeAggrFunc extends AggregateFunction {

    public MaxTimeAggrFunc() {
        super(AggregationConstant.MAX_TIME, TSDataType.INT64);
    }

    @Override
    public void putDefaultValue() {
        resultData.putEmptyTime(0);
    }

    @Override
    public void calculateValueFromPageHeader(PageHeader pageHeader) {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }

        long timestamp = pageHeader.data_page_header.max_timestamp;
        updateMaxTime(timestamp);
    }

    @Override
    public void calculateValueFromDataPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }

        if (dataInThisPage.valueLength == 0) {
            return;
        }
        long timestamp = dataInThisPage.getTime(dataInThisPage.valueLength - 1);
        updateMaxTime(timestamp);
    }

    @Override
    public void calculateValueFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }

        long time = -1;
        while (insertMemoryData.hasNext()) {
            time = insertMemoryData.getCurrentMinTime();
            insertMemoryData.removeCurrentValue();
        }

        if (time != -1) {
            updateMaxTime(time);
        }
    }

    @Override
    public boolean calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex)
            throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }

        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasNext()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    updateMaxTime(timestamps.get(timeIndex));
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

        return insertMemoryData.hasNext();
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

        long maxTime = -1;
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                if (maxTime < data.getTime(data.curIdx)) {
                    maxTime = data.getTime(data.curIdx);
                }
                data.curIdx++;
            }
        }

        if (maxTime != -1) {
            if (resultData.emptyTimeLength > 0 && resultData.getEmptyTime(resultData.emptyTimeLength - 1) == partitionStart) {
                resultData.removeLastEmptyTime();
                resultData.putTime(partitionStart);
                resultData.putLong(maxTime);
            } else {
                if (maxTime > resultData.getLong(resultData.valueLength - 1)) {
                    resultData.setLong(resultData.valueLength - 1, maxTime);
                }
            }
        }
    }

    private void updateMaxTime(long timestamp) {
        if (!hasSetValue) {
            resultData.putLong(timestamp);
            hasSetValue = true;
        } else {
            long maxt = resultData.getLong(0);
            maxt = maxt > timestamp ? maxt : timestamp;
            resultData.setLong(0, maxt);
        }
    }
}
