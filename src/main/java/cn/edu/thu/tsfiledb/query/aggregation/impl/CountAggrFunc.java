package cn.edu.thu.tsfiledb.query.aggregation.impl;

import java.io.IOException;

import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

public class CountAggrFunc extends AggregateFunction {

    public CountAggrFunc() {
        super("COUNT", TSDataType.INT64);
        result.data.putTime(0);
        result.data.putLong(0);
    }

    @Override
    public boolean calculateValueFromPageHeader(PageHeader pageHeader) {
        long preValue = result.data.getLong(0);
        preValue += pageHeader.data_page_header.num_rows;
        result.data.setLong(0, preValue);
        return true;
    }

    @Override
    public void calculateValueFromDataInThisPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        if (dataInThisPage instanceof InsertDynamicData) {
            InsertDynamicData insertMemoryData = (InsertDynamicData) dataInThisPage;
            long preValue = result.data.getLong(0);
            Object count = insertMemoryData.calcAggregation("COUNT");
            preValue += (long)count;
            result.data.setLong(0, preValue);
        } else {
            long preValue = result.data.getLong(0);
            preValue += dataInThisPage.valueLength;
            result.data.setLong(0, preValue);
        }
    }
}
