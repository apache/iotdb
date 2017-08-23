package cn.edu.thu.tsfiledb.query.aggregation.impl;

import java.io.IOException;

import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

public class MaxTimeAggrFunc extends AggregateFunction {

    private boolean hasSetValue = false;

    public MaxTimeAggrFunc() {
        super("MAX_TIME", TSDataType.INT64);
    }

    @Override
    public boolean calculateValueFromPageHeader(PageHeader pageHeader) {
        long timestamp = pageHeader.data_page_header.max_timestamp;
        if (!hasSetValue) {
            result.data.putLong(timestamp);
            hasSetValue = true;
        } else {
            long maxv = result.data.getLong(0);
            maxv = maxv > timestamp ? maxv : timestamp;
            result.data.setLong(0, maxv);
        }
        return true;
    }

    @Override
    public void calculateValueFromDataInThisPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        if (dataInThisPage instanceof InsertDynamicData) {
            Pair<Long, Object> pair = ((InsertDynamicData) dataInThisPage).calcAggregation("MAX_TIME");
            if (pair.left != 0) {
                long timestamp = (long)pair.right;
                if (!hasSetValue) {
                    result.data.putLong(timestamp);
                    hasSetValue = true;
                } else {
                    long maxv = result.data.getLong(0);
                    maxv = maxv > timestamp ? maxv : timestamp;
                    result.data.setLong(0, maxv);
                }
                long count = result.data.getTime(0) + pair.left;
                result.data.setTime(0, count);
            }
        } else {
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
    }

}
