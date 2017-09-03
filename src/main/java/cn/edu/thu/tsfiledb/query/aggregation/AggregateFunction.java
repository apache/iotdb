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

    protected abstract boolean calculateValueFromPageHeader(PageHeader pageHeader);

    /**
     * Return false if the result can not be calculated from pageHeader.
     * //TODO this method always reture true..
     *
     * @param pageHeader <code>PageHeader</code>
     * @return false if the result can not be judged from pageHeader.
     */
    public boolean couldCalculateFromPageHeader(PageHeader pageHeader) {
        return calculateValueFromPageHeader(pageHeader);
    }

    protected abstract void calculateValueFromDataInThisPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException;

    /**
     * Before invoking this method, <code>couldCalculateFromPageHeader</code> method will return false.
     * Calculate the aggregation according to all decompressed data in this page.
     *
     * @param dataInThisPage the data in the DataPage
     * @throws IOException tsfile data read excption
     * @throws ProcessorException wrong aggregation method parameter
     */
    public void calculateFromDataInThisPage(DynamicOneColumnData dataInThisPage) throws IOException, ProcessorException {
        calculateValueFromDataInThisPage(dataInThisPage);
    }

    /**
     * Before invoking this method, <code>couldCalculateFromPageHeader</code> method will return false.
     * Calculate the aggregation according to all decompressed data in this page.
     *
     * @param insertMemoryData the data in the DataPage with memory bufferwrite data
     * @throws IOException tsfile data read excption
     * @throws ProcessorException wrong aggregation method parameter
     */
    public void calculateFromLeftMemoryData(InsertDynamicData insertMemoryData) throws IOException, ProcessorException {
        calculateValueFromDataInThisPage(insertMemoryData);
    }

    /**
     * This method is calculate the aggregation using the common timestamps of cross series filter.
     *
     * @param insertMemoryData the data in the DataPage with memory bufferwrite data
     * @param timestamps the common timestamps calculated by cross series filter
     * @throws IOException tsfile data read excption
     * @throws ProcessorException wrong aggregation method parameter
     */
    public void calcAggregationUsingTimestamps(InsertDynamicData insertMemoryData, List<Long> timestamps) throws IOException, ProcessorException {

    }
}
