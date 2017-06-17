package cn.edu.thu.tsfiledb.qp.physical.crud;

import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.lineFeedSignal;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.readSupport.RowRecord;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.crud.FilterOperator;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

/**
 * This class is constructed with a single query plan. Single query means it could be processed by
 * TsFile reading API by one pass directly.<br>
 * Up to now, Single Query that {@code TsFile reading API} supports is a conjunction among time
 * filter, frequency filter and value filter. <br>
 * This class provide two public function. If the whole SeriesSelectPlan has exactly one single path,
 * {@code SeriesSelectPlan} return a {@code Iterator<QueryDataSet>} directly. Otherwise
 * {@code SeriesSelectPlan} is regard as a portion of {@code MergeQuerySetPlan}. This class provide
 * a {@code Iterator<RowRecord>}in the latter case.
 *
 * @author kangrong
 */
public class SeriesSelectPlan extends PhysicalPlan {

    private static final Logger LOG = LoggerFactory.getLogger(SeriesSelectPlan.class);
    private List<Path> paths = new ArrayList<>();
    private FilterOperator timeFilterOperator;
    private FilterOperator freqFilterOperator;
    private FilterOperator valueFilterOperator;

    public FilterOperator getTimeFilterOperator() {
        return timeFilterOperator;
    }

    public FilterOperator getFreqFilterOperator() {
        return freqFilterOperator;
    }

    public FilterOperator getValueFilterOperator() {
        return valueFilterOperator;
    }

    public SeriesSelectPlan(List<Path> paths,
                            FilterOperator timeFilter, FilterOperator freqFilter, FilterOperator valueFilter, QueryProcessExecutor executor) {
        super(true, OperatorType.QUERY);
        this.paths = paths;
        this.timeFilterOperator = timeFilter;
        this.freqFilterOperator = freqFilter;
        this.valueFilterOperator = valueFilter;
        removeStarsInPath(executor);
        LOG.debug(Arrays.toString(paths.toArray()));
        removeNotExistsPaths(executor);
        LOG.debug(Arrays.toString(paths.toArray()));
    }

    @Override
    public Iterator<QueryDataSet> processQuery(QueryProcessExecutor executor) throws QueryProcessorException {
        FilterExpression[] expressions = transformToFilterExpressions(executor);

        return new QueryDataSetIterator(paths, executor.getFetchSize(), executor, expressions[0], expressions[1],
                expressions[2]);
    }

    private void removeStarsInPath(QueryProcessExecutor executor) {
        LinkedHashMap<String, Integer> pathMap = new LinkedHashMap<>();
        for (Path path : paths) {
            List<String> all;
            try {
                all = executor.getAllPaths(path.getFullPath());
                for(String subp : all){
                    if(!pathMap.containsKey(subp)){
                        pathMap.put(subp, 1);
                    }
                }
            } catch (PathErrorException e) {
                LOG.error("path error:" + e.getMessage());
            }
        }
        paths = new ArrayList<>();
        for (String pathStr : pathMap.keySet()) {
            paths.add(new Path(pathStr));
        }
    }

    private void removeNotExistsPaths(QueryProcessExecutor executor) {
        List<Path> existsPaths = new ArrayList<>();
        List<Path> notExistsPaths = new ArrayList<>();
        for (Path path : paths) {
            if (executor.judgePathExists(path))
                existsPaths.add(path);
            else
                notExistsPaths.add(path);
        }
        if (!notExistsPaths.isEmpty()) {
            LOG.warn("following paths don't exist:{}", notExistsPaths.toString());
        }
        this.paths = existsPaths;

    }

    private FilterExpression[] transformToFilterExpressions(QueryProcessExecutor conf)
            throws QueryProcessorException {
        FilterExpression timeFilter =
                timeFilterOperator == null ? null : timeFilterOperator.transformToFilterExpression(conf, FilterSeriesType.TIME_FILTER);
        FilterExpression freqFilter =
                freqFilterOperator == null ? null : freqFilterOperator.transformToFilterExpression(conf, FilterSeriesType.FREQUENCY_FILTER);
        FilterExpression valueFilter =
                valueFilterOperator == null ? null : valueFilterOperator.transformToFilterExpression(conf, FilterSeriesType.VALUE_FILTER);

        if (valueFilter instanceof SingleSeriesFilterExpression) {
            if (paths.size() == 1) {
                FilterSeries<?> series = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries();
                Path path = paths.get(0);
                if (!series.getDeltaObjectUID().equals(path.getDeltaObjectToString())
                        || !series.getMeasurementUID().equals(path.getMeasurementToString())) {
                    valueFilter = FilterFactory.csAnd(valueFilter, valueFilter);
                }
            } else
                valueFilter = FilterFactory.csAnd(valueFilter, valueFilter);
        }
        return new FilterExpression[]{timeFilter, freqFilter, valueFilter};
    }

    /**
     * provide {@code Iterator<RowRecord>} for
     * {@link MergeQuerySetIterator} which has more than one
     * {@code SeriesSelectPlan} to merge
     *
     * @param executor
     * @return Iterator<RowRecord>
     */
    private Iterator<RowRecord> getRecordIterator(QueryProcessExecutor executor) throws QueryProcessorException {
        FilterExpression[] filterExpressions = transformToFilterExpressions(executor);

        return new RowRecordIterator(executor.getFetchSize(), executor, filterExpressions[0], filterExpressions[1], filterExpressions[2]);
    }


    public static Iterator<RowRecord>[] getRecordIteratorArray(List<SeriesSelectPlan> plans,
                                                               QueryProcessExecutor conf) throws QueryProcessorException {
        Iterator<RowRecord>[] ret = new RowRecordIterator[plans.size()];
        for (int i = 0; i < plans.size(); i++) {
            ret[i] = plans.get(i).getRecordIterator(conf);
        }
        return ret;
    }

    public class RowRecordIterator implements Iterator<RowRecord> {
        private boolean noNext = false;
        private final int fetchSize;
        private final QueryProcessExecutor conf;
        private QueryDataSet data = null;
        private FilterExpression timeFilter;
        private FilterExpression freqFilter;
        private FilterExpression valueFilter;

        public RowRecordIterator(int fetchSize, QueryProcessExecutor conf,
                                 FilterExpression timeFilter, FilterExpression freqFilter,
                                 FilterExpression valueFilter) {
            this.fetchSize = fetchSize;
            this.conf = conf;
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
                    data = conf.query(paths, timeFilter, freqFilter, valueFilter, fetchSize, data);
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

    private class QueryDataSetIterator implements Iterator<QueryDataSet> {
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
                    throw new RuntimeException(e.getMessage());
                }
            if (data == null) {
                LOG.error(
                        "data is null! parameters: paths:{},timeFilter:{}, freqFilter:{}, valueFilter:{}, fetchSize:{}, usedData:{}",
                        paths, timeFilter, freqFilter, valueFilter, fetchSize, usedData);
                throw new RuntimeException("data is null! parameters: paths:");
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


    @Override
    public String printQueryPlan() {
        StringContainer sc = new StringContainer();
        String preSpace = "  ";
        sc.addTail(preSpace, "series getIndex plan:", lineFeedSignal);
        sc.addTail(preSpace, "paths:  ").addTail(paths.toString(), lineFeedSignal);
        sc.addTail(preSpace, timeFilterOperator == null ? "null" : timeFilterOperator.toString(),
                lineFeedSignal);
        sc.addTail(preSpace, freqFilterOperator == null ? "null" : freqFilterOperator.toString(),
                lineFeedSignal);
        sc.addTail(preSpace, valueFilterOperator == null ? "null" : valueFilterOperator.toString(),
                lineFeedSignal);
        return sc.toString();
    }

    @Override
    public List<Path> getPaths() {
        return paths;
    }
}
