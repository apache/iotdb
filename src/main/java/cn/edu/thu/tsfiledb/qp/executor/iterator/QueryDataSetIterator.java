package cn.edu.thu.tsfiledb.qp.executor.iterator;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class QueryDataSetIterator implements Iterator<QueryDataSet> {
    private static final Logger logger = LoggerFactory.getLogger(QueryDataSetIterator.class);

    private boolean noNext = false;
    private final int fetchSize;
    private final QueryProcessExecutor conf;
    private QueryDataSet data = null;
    private QueryDataSet usedData = null;
    private FilterExpression timeFilter;
    private FilterExpression freqFilter;
    private FilterExpression valueFilter;
    private List<Path> paths;

    public QueryDataSetIterator(List<Path> paths, int fetchSize, QueryProcessExecutor conf,
                                FilterExpression timeFilter, FilterExpression freqFilter,
                                FilterExpression valueFilter) {
        this.paths = paths;
        this.fetchSize = fetchSize;
        this.conf = conf;
        this.timeFilter = timeFilter;
        this.freqFilter = freqFilter;
        this.valueFilter = valueFilter;
    }

    @Override
    public boolean hasNext() {
        if (usedData != null) {
            usedData.clear();
        }
        if (noNext)
            return false;
        if (data == null || !data.hasNextRecord())
            try {
                data = conf.query(paths, timeFilter, freqFilter, valueFilter, fetchSize, usedData);
            } catch (ProcessorException e) {
            		logger.error("meet error in hasNext,", e);
                throw new RuntimeException(e.getMessage());
            }
        if (data == null) {
            logger.error(
                    "data is null! parameters: paths:{},timeFilter:{}, freqFilter:{}, valueFilter:{}, fetchSize:{}, usedData:{}",
                    paths, timeFilter, freqFilter, valueFilter, fetchSize, usedData);
            throw new RuntimeException("data is null! parameters: paths:" + paths);
        }
        if (data.hasNextRecord())
            return true;
        else {
            noNext = true;
            return false;
        }
    }

    @Override
    public QueryDataSet next() {
        usedData = data;
        data = null;
        return usedData;
    }
}