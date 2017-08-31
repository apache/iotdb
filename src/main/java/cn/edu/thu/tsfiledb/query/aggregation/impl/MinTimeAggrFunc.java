package cn.edu.thu.tsfiledb.query.aggregation.impl;

import java.io.IOException;

import cn.edu.thu.tsfiledb.query.aggregation.AggregateFunction;
import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

public class MinTimeAggrFunc extends AggregateFunction {

    private boolean hasSetValue = false;

    public MinTimeAggrFunc() {
        super("MIN_TIME", TSDataType.INT64);
        result.data.putTime(0);
    }

    @Override
    public boolean calculateValueFromPageHeader(PageHeader pageHeader) {
        long timestamp = pageHeader.data_page_header.min_timestamp;
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
            Object min_time = ((InsertDynamicData) dataInThisPage).calcAggregation("MIN_TIME");
            if (min_time != null) {
                long timestamp = (long)min_time;
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
        else {
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
    }

}
