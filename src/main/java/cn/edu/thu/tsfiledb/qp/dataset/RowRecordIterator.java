package cn.edu.thu.tsfiledb.qp.dataset;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.readSupport.RowRecord;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;

import java.util.Iterator;
import java.util.List;

public class RowRecordIterator implements Iterator<RowRecord> {
    private boolean noNext = false;
    private List<Path> paths;
    private final int fetchSize;
    private final QueryProcessExecutor executor;
    private QueryDataSet data = null;
    private FilterExpression timeFilter;
    private FilterExpression freqFilter;
    private FilterExpression valueFilter;

    public RowRecordIterator(List<Path> paths, int fetchSize, QueryProcessExecutor executor,
                             FilterExpression timeFilter, FilterExpression freqFilter,
                             FilterExpression valueFilter) {
        this.paths = paths;
        this.fetchSize = fetchSize;
        this.executor = executor;
        this.timeFilter = timeFilter;
        this.freqFilter = freqFilter;
        this.valueFilter = valueFilter;
    }

    @Override
    public boolean hasNext() {
        if (noNext)
            return false;
        if (data == null || !data.hasNextRecord())
            try {
                data = executor.query(paths, timeFilter, freqFilter, valueFilter, fetchSize, data);
            } catch (ProcessorException e) {
                throw new RuntimeException(e.getMessage());
            }
        if (data.hasNextRecord())
            return true;
        else {
            noNext = true;
            return false;
        }
    }

    @Override
    public RowRecord next() {
        return data.getNextRecord();
    }

}