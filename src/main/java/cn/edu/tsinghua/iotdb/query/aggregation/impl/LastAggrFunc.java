package cn.edu.tsinghua.iotdb.query.aggregation.impl;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.Digest;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.StrDigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class LastAggrFunc extends AggregateFunction {

    private boolean hasSetValue = false;

    public LastAggrFunc(TSDataType dataType) {
        super(AggregationConstant.LAST, dataType);
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

        Digest pageDigest = pageHeader.data_page_header.getDigest();

        // TODO : need to convert to a static method?
        DigestForFilter digest = new DigestForFilter(pageDigest.getStatistics().get(AggregationConstant.LAST),
                pageDigest.getStatistics().get(AggregationConstant.LAST), dataType);
        Comparable<?> val = digest.getMaxValue();
        updateLast(val);
    }

    @Override
    public void calculateValueFromDataPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }

        if (dataInThisPage.valueLength == 0) {
            return;
        }
        updateLast(dataInThisPage);
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
        long time = -1;
        Object val = null;
        // TODO : is there easier way to get the last value ?
        while(insertMemoryData.hasInsertData()) {
            time = insertMemoryData.getCurrentMinTime();
            val = insertMemoryData.getCurrentObjectValue();
        }
        if(time > 0) {
            updateLast((Comparable<?>)val);
        }
    }

    @Override
    public boolean calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex)
            throws IOException {
        if (resultData.timeLength == 0) {
            resultData.putTime(0);
        }

        // TODO : can I traverse from the end ?
        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    Object value = insertMemoryData.getCurrentObjectValue();
                    updateLast((Comparable<?>)value);
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

        Comparable<?> lastValue = null;
        long lastTime = -1;
        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                if(time > lastTime) {
                    lastValue = data.getAnObject(data.curIdx);
                    lastTime = time;
                }
                data.curIdx++;
            }
        }

        if (lastValue != null) {
            if (resultData.emptyTimeLength > 0 && resultData.getEmptyTime(resultData.emptyTimeLength - 1) == partitionStart) {
                resultData.removeLastEmptyTime();
                resultData.putTime(partitionStart);
                resultData.putAnObject(lastValue);
            } else {
                resultData.setAnObject(resultData.valueLength - 1, lastValue);
            }
        }
    }

    private void updateLast(Comparable<?> lastVal) {
        if (!hasSetValue) {
            resultData.putAnObject(lastVal);
            hasSetValue = true;
        } else  {
            resultData.setAnObject(0, lastVal);
        }
    }

    private void updateLast(DynamicOneColumnData dataInThisPage) {
        // assert : timeLength == valueLength
        int index = dataInThisPage.timeLength - 1;
        Comparable<?> lastVal = dataInThisPage.getAnObject(index);
        updateLast(lastVal);
    }

}
