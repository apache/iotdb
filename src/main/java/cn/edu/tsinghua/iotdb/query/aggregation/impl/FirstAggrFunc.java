package cn.edu.tsinghua.iotdb.query.aggregation.impl;

import cn.edu.tsinghua.iotdb.query.aggregation.AggregateFunction;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class FirstAggrFunc extends AggregateFunction{

    public FirstAggrFunc(TSDataType dataType) {
        super(AggregationConstant.FIRST, dataType);
    }

    @Override
    public void putDefaultValue() {
        resultData.putEmptyTime(0);
    }

    @Override
    public void calculateValueFromPageHeader(PageHeader pageHeader) throws ProcessorException {
        if(resultData.timeLength == 0)
            initFirst();
        if (resultData.getTime(0) != -1) {
            return;
        }
        ByteBuffer firstVal = pageHeader.data_page_header.digest.getStatistics().get(AggregationConstant.FIRST);
        if(firstVal == null)
            throw new ProcessorException("PageHeader contains no FIRST value");
        resultData.setTime(0, 0);
        DigestForFilter digestForFilter = new DigestForFilter(firstVal, firstVal, dataType);
        resultData.putAnObject(digestForFilter.getMaxValue());
    }

    @Override
    public void calculateValueFromDataPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        //logger.error("Using page to aggregate");
        if(resultData.timeLength == 0)
            initFirst();
        if (resultData.getTime(0) != -1 || dataInThisPage.timeLength == 0) {
            return;
        }

        resultData.setTime(0, 0);
        resultData.putAnObject(dataInThisPage.getAnObject(0));
    }

    @Override
    public int calculateValueFromDataPage(DynamicOneColumnData dataInThisPage, List<Long> timestamps, int timeIndex) {
        return 0;
    }

    @Override
    public void calculateValueFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        //logger.error("Using memory to aggregate");
        if(resultData.timeLength == 0)
            initFirst();
        if (resultData.getTime(0) != -1 || insertMemoryData.timeLength == 0) {
            return;
        }

        resultData.setTime(0, 0);
        resultData.putAnObject(insertMemoryData.getCurrentObjectValue());
    }

    @Override
    public boolean calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps, int timeIndex) throws IOException, ProcessorException {
        //logger.error("Using timestamps to aggregate");
        if(resultData.timeLength == 0)
            initFirst();
        if (resultData.getTime(0) != -1) {
            return insertMemoryData.hasInsertData();
        }
        //logger.error("Finding points with timestamps");
        while (timeIndex < timestamps.size()) {
            if (insertMemoryData.hasInsertData()) {
                if (timestamps.get(timeIndex) == insertMemoryData.getCurrentMinTime()) {
                    resultData.setTime(0, 0);
                    resultData.putAnObject(insertMemoryData.getCurrentObjectValue());
                    insertMemoryData.removeCurrentValue();
                    return insertMemoryData.hasInsertData();
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
    public void calcGroupByAggregation(long partitionStart, long partitionEnd, long intervalStart, long intervalEnd, DynamicOneColumnData data) {
        if (resultData.emptyTimeLength == 0) {
            if (resultData.timeLength == 0) {
                resultData.putEmptyTime(partitionStart);
            } else if (resultData.getTime(resultData.timeLength - 1) != partitionStart) {
                resultData.putEmptyTime(partitionStart);
            } else {
                return;
            }
        } else {
            if ((resultData.getEmptyTime(resultData.emptyTimeLength - 1) != partitionStart)
                    && (resultData.timeLength == 0 ||
                    (resultData.timeLength > 0 && resultData.getTime(resultData.timeLength - 1) != partitionStart))) {
                resultData.putEmptyTime(partitionStart);
            } else if(resultData.timeLength > 0 && resultData.getTime(resultData.timeLength - 1) == partitionStart){
                return;
            }
        }

        while (data.curIdx < data.timeLength) {
            long time = data.getTime(data.curIdx);
            if (time > intervalEnd || time > partitionEnd) {
                break;
            } else if (time < intervalStart || time < partitionStart) {
                data.curIdx++;
            } else if (time >= intervalStart && time <= intervalEnd && time >= partitionStart && time <= partitionEnd) {
                resultData.removeLastEmptyTime();
                resultData.putTime(partitionStart);
                resultData.putAnObject(data.getAnObject(data.curIdx));
                data.curIdx++;
                return;
            }
        }
    }

    // add a place holder
    private void initFirst() {
        resultData.putTime(-1);
    }
}
