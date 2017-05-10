package cn.edu.thu.tsfiledb.qp.physical.plan.query;

import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.lineFeedSignal;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.readSupport.RowRecord;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.FilterOperator;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

/**
 * This class is constructed with a single getIndex plan. Single getIndex means it could be processed by
 * reading API by one pass directly.<br>
 * Up to now, Single Query what {@code reading API} supports means it's a conjunction among time
 * filter, frequency filter and value filter. <br>
 * This class provide two public function. If the whole getIndex plan has exactly one single getIndex,
 * {@code SeriesSelectPlan} return a {@code Iterator<QueryDataSet>} directly. Otherwise
 * {@code SeriesSelectPlan} is regard as a portion of {@code MergeQuerySetPlan}. This class provide
 * a {@code Iterator<RowRecord>}in the latter case.
 * 
 * @author kangrong
 *
 */
public class SeriesSelectPlan extends PhysicalPlan {
    private static final Logger LOG = LoggerFactory.getLogger(SeriesSelectPlan.class);
    private List<Path> paths;
    private FilterOperator deltaObjectFilterOperator;

    public FilterOperator getDeltaObjectFilterOperator() {
        return deltaObjectFilterOperator;
    }

    public void setDeltaObjectFilterOperator(FilterOperator deltaObjectFilterOperator) {
        this.deltaObjectFilterOperator = deltaObjectFilterOperator;
    }

    private FilterOperator timeFilterOperator;
    private FilterOperator freqFilterOperator;
    private FilterOperator valueFilterOperator;

    public List<Path> getPaths() {
        return paths;
    }

    public FilterOperator getTimeFilterOperator() {
        return timeFilterOperator;
    }

    public FilterOperator getFreqFilterOperator() {
        return freqFilterOperator;
    }

    public FilterOperator getValueFilterOperator() {
        return valueFilterOperator;
    }

    public SeriesSelectPlan(List<Path> paths, FilterOperator timeFilter, FilterOperator freqFilter,
            FilterOperator valueFilter, QueryProcessExecutor conf) {
        this(paths, null, timeFilter, freqFilter, valueFilter, conf);
    }

    public SeriesSelectPlan(List<Path> paths, FilterOperator deltaObjectFilterOperator,
            FilterOperator timeFilter, FilterOperator freqFilter, FilterOperator valueFilter, QueryProcessExecutor conf) {
        super(true, OperatorType.QUERY);
        this.paths = paths;
        this.deltaObjectFilterOperator = deltaObjectFilterOperator;
        this.timeFilterOperator = timeFilter;
        this.freqFilterOperator = freqFilter;
        this.valueFilterOperator = valueFilter;
        removeStarsInPath(conf);
        LOG.info(Arrays.toString(paths.toArray()));
        removeNotExistsPaths(conf);
        LOG.info(Arrays.toString(paths.toArray()));
    }
    
    @Override
    public Iterator<QueryDataSet> processQuery(QueryProcessExecutor conf) {
        FilterExpression[] exprs;
        try {
            exprs = transformFilterOpToExpression(conf);
        } catch (QueryProcessorException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        return new QueryDataSetIterator(paths, conf.getFetchSize(), conf, exprs[0], exprs[1],
                exprs[2]);
    }

    private void removeStarsInPath(QueryProcessExecutor conf) {
    	LinkedHashMap<String, Integer> pathMap = new LinkedHashMap<>();
        for (Path path : paths) {
            List<String> all = null;
            try {
                all = conf.getAllPaths(path.getFullPath());
                for(String subp : all){
                 	if(!pathMap.containsKey(subp)){
                 		pathMap.put(subp, 1);
                 	}
                }
            } catch (PathErrorException e) {
                LOG.error("path error:" + e.getMessage());
                continue;
            }
        }
        paths = new ArrayList<Path>();
        for (String pathStr : pathMap.keySet()) {
            paths.add(new Path(pathStr));
        }
    }

    private void removeNotExistsPaths(QueryProcessExecutor conf) {
        List<Path> existsPaths = new ArrayList<Path>();
        List<Path> notExistsPaths = new ArrayList<Path>();
        for (Path path : paths) {
            if (conf.judgePathExists(path))
                existsPaths.add(path);
            else
                notExistsPaths.add(path);
        }
        if (!notExistsPaths.isEmpty()) {
            LOG.warn("following paths don't exist:{}", notExistsPaths.toString());
        }
        this.paths = existsPaths;

    }

    private FilterExpression[] transformFilterOpToExpression(QueryProcessExecutor conf)
            throws QueryProcessorException {
        FilterExpression timeFilter =
                timeFilterOperator == null ? null : timeFilterOperator.transformToFilter(conf);
        FilterExpression freqFilter =
                freqFilterOperator == null ? null : freqFilterOperator.transformToFilter(conf);
        FilterExpression valueFilter =
                valueFilterOperator == null ? null : valueFilterOperator.transformToFilter(conf);
        // TODO maybe it's a temporary solution. Up to now, if a crossSensorFilter is needed, just
        // construct it with two same children via CSAnd(valueFilter, valueFilter)
        if (valueFilter instanceof SingleSeriesFilterExpression) {
            if (paths.size() == 1) {
                FilterSeries<?> series = ((SingleSeriesFilterExpression) valueFilter).getFilterSeries();
                Path path = paths.get(0);
                if (!series.getDeltaObjectUID().equals(path.getDeltaObjectToString())
                        || !series.getMeasurementUID().equals(path.getMeasurementToString())) {
                    valueFilter = FilterFactory.and(valueFilter, valueFilter);
                }
            } else
                valueFilter = FilterFactory.and(valueFilter, valueFilter);
        }
        return new FilterExpression[] {timeFilter, freqFilter, valueFilter};
    }


    /**
     * provide {@code Iterator<RowRecord>} for
     * {@link cn.edu.thu.tsfiledb.qp.physical.plan.query.MergeQuerySetIterator} which has more than one
     * {@code SeriesSelectPlan} to merge
     * 
     * @param conf
     * @return
     */
    private Iterator<RowRecord> getRecordIterator(QueryProcessExecutor conf) {
        FilterExpression[] exprs = null;
        try {
            exprs = transformFilterOpToExpression(conf);
        } catch (QueryProcessorException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        return new RowRecordIterator(conf.getFetchSize(), conf, exprs[0], exprs[1], exprs[2]);
    }

    /**
     * provide {@code Iterator<QueryDataSet>} for
     * {@link com.corp.tsfile.qp.physical.plan.MergeQuerySetIterator} which has exactly one
     * {@code SeriesSelectPlan}.
     * 
     * @param conf
     * @return
     */
    // public Iterator<QueryDataSet> getQueryDataSetIterator(QueryProcessExecutor conf) {
    // return new QueryDataSetIterator(conf.getFetchSize(), conf);
    // }

    /**
     * only used by
     * 
     * @param plans
     * @param conf
     * @return
     */
    public static Iterator<RowRecord>[] getRecordIteratorArray(List<SeriesSelectPlan> plans,
            QueryProcessExecutor conf) {
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
        public boolean hasNext(){
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
    public List<Path> getInvolvedSeriesPaths() {
        if (paths == null)
            return new ArrayList<Path>();
        else
            return paths;
    }
}
