package cn.edu.thu.tsfiledb.query.aggregation;

import java.io.IOException;
import java.util.List;

import cn.edu.thu.tsfiledb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

public abstract class AggregateFunction {

    public String name;
    public AggregationResult result;
    public TSDataType dataType;

    public AggregateFunction(String name, TSDataType dataType) {
        this.name = name;
        this.dataType = dataType;
        result = new AggregationResult(dataType);
    }

    /**
     * Return false if the result can not be calculated from pageHeader.
     */
    public abstract boolean calculateValueFromPageHeader(PageHeader pageHeader);

    /**
     * Calculate the value according to all data in this page.
     */
    public abstract void calculateValueFromDataInThisPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException;

    public boolean couldCalculateFromPageHeader(PageHeader pageHeader) {
        return calculateValueFromPageHeader(pageHeader);
    }

    public void calculateFromDataInThisPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        calculateValueFromDataInThisPage(dataInThisPage);
    }

    public void calculateFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        calculateValueFromDataInThisPage(insertMemoryData);
    }

    // for cross series multi aggregate
    public void calcMemoryUseTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps) throws IOException, ProcessorException {

    }
}
