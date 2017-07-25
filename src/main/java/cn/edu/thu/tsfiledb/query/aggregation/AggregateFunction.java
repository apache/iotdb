package cn.edu.thu.tsfiledb.query.aggregation;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.format.PageHeader;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;

import java.io.IOException;

public abstract class AggregateFunction {

    public String name;
    public AggregationResult result;
    public TSDataType dataType;

    public AggregateFunction(String name, TSDataType dataType) {
        this.name = name;
        this.dataType = dataType;
        result = new AggregationResult(dataType);
        result.data.putTime(0);
    }

    /**
     * Return false if the result can not be calculated from pageHeader.
     */
    public abstract boolean calculateValueFromPageHeader(PageHeader pageHeader);

    /**
     * Calculate the value according to all data in this page.
     */
    public abstract void calculateValueFromDataInThisPage(DynamicOneColumnData dataInThisPage) throws IOException;

    public boolean calculateFromPageHeader(PageHeader pageHeader) {
        boolean ret = calculateValueFromPageHeader(pageHeader);
        if (ret) {
            addCount(pageHeader);
        }
        return ret;
    }

    public void calculateFromDataInThisPage(DynamicOneColumnData dataInThisPage) throws IOException {
        calculateValueFromDataInThisPage(dataInThisPage);
        addCount(dataInThisPage);
    }

    public void calculateFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException {
        calculateValueFromDataInThisPage(insertMemoryData);
        // addCount(insertMemoryData);
    }

    private void addCount(PageHeader pageHeader) {
        long count = result.data.getTime(0) + pageHeader.data_page_header.num_rows;
        result.data.setTime(0, count);
    }

    private void addCount(DynamicOneColumnData dataInThisPage) {
        if (dataInThisPage instanceof InsertDynamicData) {

        } else {
            long count = result.data.getTime(0) + dataInThisPage.valueLength;
            result.data.setTime(0, count);
        }
    }

}
