package cn.edu.tsinghua.iotdb.qp.physical.crud;

import static cn.edu.tsinghua.iotdb.qp.constant.SQLConstant.lineFeedSignal;
import java.util.*;

import cn.edu.tsinghua.iotdb.qp.executor.QueryProcessExecutor;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.logical.crud.FilterOperator;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;

/**
 * This class is constructed with a single query plan. Single query means it could be processed by
 * TsFile reading API by one pass directly.<br>
 * Up to now, Single Query that {@code TsFile reading API} supports is a conjunction among time
 * filter, frequency filter and value filter. <br>
 * This class provide two public function. If the whole SingleQueryPlan has exactly one single path,
 * {@code SingleQueryPlan} return a {@code Iterator<QueryDataSet>} directly. Otherwise
 * {@code SingleQueryPlan} is regard as a portion of {@code MultiQueryPlan}. This class provide
 * a {@code Iterator<RowRecord>}in the latter case.
 *
 */
public class SingleQueryPlan extends PhysicalPlan {

    private static final Logger LOG = LoggerFactory.getLogger(SingleQueryPlan.class);
    private List<Path> paths = new ArrayList<>();
    private List<String> aggregations = new ArrayList<>();
    private FilterOperator timeFilterOperator;
    private FilterOperator freqFilterOperator;
    private FilterOperator valueFilterOperator;
    private FilterExpression[] filterExpressions;

    public SingleQueryPlan(List<Path> paths, FilterOperator timeFilter,
                           FilterOperator freqFilter, FilterOperator valueFilter,
                           QueryProcessExecutor executor, List<String> aggregations) throws QueryProcessorException {
        super(true, Operator.OperatorType.QUERY);
        this.paths = paths;
        this.timeFilterOperator = timeFilter;
        this.freqFilterOperator = freqFilter;
        this.valueFilterOperator = valueFilter;
        checkPaths(executor);
        filterExpressions = transformToFilterExpressions(executor);
        this.aggregations = aggregations;
    }

    public void setAggregations(List<String> aggregations) {
        this.aggregations = aggregations;
    }

    public List<String> getAggregations() {
        return aggregations;
    }

    /**
     * filterExpressions include three FilterExpression: TIME_FILTER, FREQUENCY_FILTER, VALUE_FILTER
     * These filters is for querying data in TsFile
     *
     * @return three filter expressions
     */
    public FilterExpression[] getFilterExpressions() {
        return filterExpressions;
    }

    /**
     * check if all paths exist
     */
    private void checkPaths(QueryProcessExecutor executor) throws QueryProcessorException {
        for (Path path : paths) {
            if (!executor.judgePathExists(path))
                throw new QueryProcessorException("Path doesn't exist: " + path);
        }
    }

    /**
     * convert filter operators to filter expressions
     *
     * @param executor query process executor
     * @return three filter expressions
     * @throws QueryProcessorException exceptions in transforming filter operators
     */
    private FilterExpression[] transformToFilterExpressions(QueryProcessExecutor executor)
            throws QueryProcessorException {
        FilterExpression timeFilter =
                timeFilterOperator == null ? null : timeFilterOperator.transformToFilterExpression(executor, FilterSeriesType.TIME_FILTER);
        FilterExpression freqFilter =
                freqFilterOperator == null ? null : freqFilterOperator.transformToFilterExpression(executor, FilterSeriesType.FREQUENCY_FILTER);
        FilterExpression valueFilter =
                valueFilterOperator == null ? null : valueFilterOperator.transformToFilterExpression(executor, FilterSeriesType.VALUE_FILTER);

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
     * @param executor query process executor
     * @return Iterator<RowRecord>
     */
    private Iterator<RowRecord> getRecordIterator(QueryProcessExecutor executor, int formNumber) throws QueryProcessorException {

        return new RowRecordIterator(formNumber,paths, executor.getFetchSize(), executor, filterExpressions[0], filterExpressions[1], filterExpressions[2]);
    }


    public static Iterator<RowRecord>[] getRecordIteratorArray(List<SingleQueryPlan> plans,
                                                               QueryProcessExecutor conf) throws QueryProcessorException {
        Iterator<RowRecord>[] ret = new RowRecordIterator[plans.size()];
        for (int i = 0; i < plans.size(); i++) {
            ret[i] = plans.get(i).getRecordIterator(conf, i);
        }
        return ret;
    }

    @Override
    public String printQueryPlan() {
        StringContainer sc = new StringContainer();
        String preSpace = "  ";
        sc.addTail("SingleQueryPlan:", lineFeedSignal);
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

    private class RowRecordIterator implements Iterator<RowRecord> {
        private boolean noNext = false;
        private List<Path> paths;
        private final int fetchSize;
        private final QueryProcessExecutor executor;
        private QueryDataSet data = null;
        private FilterExpression timeFilter;
        private FilterExpression freqFilter;
        private FilterExpression valueFilter;
        private int formNumber;

        public RowRecordIterator(int formNumber, List<Path> paths, int fetchSize, QueryProcessExecutor executor,
                                 FilterExpression timeFilter, FilterExpression freqFilter,
                                 FilterExpression valueFilter) {
            this.formNumber = formNumber;
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
                    data = executor.query(formNumber, paths, timeFilter, freqFilter, valueFilter, fetchSize, data);
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
}
