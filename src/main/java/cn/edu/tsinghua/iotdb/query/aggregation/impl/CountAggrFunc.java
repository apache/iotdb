package cn.edu.tsinghua.iotdb.query.aggregation.impl;

import java.io.IOException;

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
        result.data.putTime(0);
        result.data.putLong(0);
    }

    @Override
    public void calculateValueFromPageHeader(PageHeader pageHeader) {
        long preValue = result.data.getLong(0);
        preValue += pageHeader.data_page_header.num_rows;
        result.data.setLong(0, preValue);
    }

    @Override
    public void calculateValueFromDataPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        long preValue = result.data.getLong(0);
        preValue += dataInThisPage.valueLength;
        result.data.setLong(0, preValue);
    }

    @Override
    public void calculateValueFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        long preValue = result.data.getLong(0);
        Object count = insertMemoryData.calcAggregation(AggregationConstant.COUNT);
        preValue += (long) count;
        result.data.setLong(0, preValue);
    }
}
