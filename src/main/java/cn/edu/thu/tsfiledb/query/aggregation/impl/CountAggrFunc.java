package cn.edu.thu.tsfiledb.query.aggregation.impl;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.format.PageHeader;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;

import java.io.IOException;

public class CountAggrFunc extends AggregateFunction {

    public CountAggrFunc() {
        super("COUNT", TSDataType.INT64);
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
    public void calculateValueFromDataInThisPage(DynamicOneColumnData dataInThisPage) throws IOException {
        if (dataInThisPage instanceof InsertDynamicData) {
            InsertDynamicData insertMemoryData = (InsertDynamicData) dataInThisPage;
            long preValue = result.data.getLong(0);
            // preValue += insertMemoryData.getValuesNumber();
            Pair<Long, Object> pair = insertMemoryData.calcAggregation("COUNT");
            preValue += pair.left;
            long count = result.data.getTime(0) + (long)pair.right;
            result.data.setTime(0, count);
            result.data.setLong(0, preValue);
        } else {
            long preValue = result.data.getLong(0);
            preValue += dataInThisPage.valueLength;
            result.data.setLong(0, preValue);
        }
    }
}
