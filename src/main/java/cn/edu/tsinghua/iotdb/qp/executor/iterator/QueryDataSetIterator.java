package cn.edu.tsinghua.iotdb.qp.executor.iterator;

import cn.edu.tsinghua.iotdb.qp.executor.QueryProcessExecutor;
import cn.edu.tsinghua.iotdb.qp.physical.crud.MultiQueryPlan;
import cn.edu.tsinghua.iotdb.query.engine.FilterStructure;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class QueryDataSetIterator implements Iterator<QueryDataSet> {

    private boolean noNext = false;
    private final int fetchSize;
    private final QueryProcessExecutor executor;
    private QueryDataSet data = null;
    private QueryDataSet usedData = null;
    private List<Path> paths;
    private List<String> aggregations;
    private List<FilterStructure> filterStructures = new ArrayList<>();
    private long unit;
    private long origin;
    private List<Pair<Long, Long>> intervals;
    private MultiQueryPlan.QueryType type = MultiQueryPlan.QueryType.QUERY;

    //single query
    public QueryDataSetIterator(List<Path> paths, int fetchSize, QueryProcessExecutor executor,
                                FilterExpression timeFilter, FilterExpression freqFilter,
                                FilterExpression valueFilter) {
        this.paths = paths;
        this.fetchSize = fetchSize;
        this.executor = executor;
        this.filterStructures.add(new FilterStructure(timeFilter, freqFilter, valueFilter));
        this.aggregations = null;
        this.type = MultiQueryPlan.QueryType.QUERY;
    }

    //aggregation
    public QueryDataSetIterator(List<Path> paths, int fetchSize, List<String> aggregations,
                                List<FilterStructure> filterStructures, QueryProcessExecutor executor) {
        this.fetchSize = fetchSize;
        this.executor = executor;
        this.filterStructures = filterStructures;
        this.paths = paths;
        this.aggregations = aggregations;
        this.type = MultiQueryPlan.QueryType.AGGREGATION;
    }

    //groupby
    public QueryDataSetIterator(List<Path> paths, int fetchSize, List<String> aggregations,
                                List<FilterStructure> filterStructures, long unit, long origin,
                                List<Pair<Long, Long>> intervals, QueryProcessExecutor executor) {
        this.fetchSize = fetchSize;
        this.executor = executor;
        this.filterStructures = filterStructures;
        this.paths = paths;
        this.aggregations = aggregations;
        this.unit = unit;
        this.origin = origin;
        this.intervals = intervals;
        this.type = MultiQueryPlan.QueryType.GROUPBY;
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
                switch (type) {
                    case QUERY:
                        FilterStructure filterStructure = filterStructures.get(0);
                        data = executor.query(0, paths, filterStructure.getTimeFilter(), filterStructure.getFrequencyFilter(),
                                filterStructure.getValueFilter(), fetchSize, usedData);
                        break;
                    case AGGREGATION:
                        data = executor.aggregate(getAggrePair(), filterStructures);
                        break;
                    case GROUPBY:
                        data = executor.groupBy(getAggrePair(), filterStructures, unit, origin, intervals, fetchSize);
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("meet error in hasNext because " + e.getMessage());
            }
        if (data == null) {
            throw new RuntimeException("data is null! parameters: paths:" + paths);
        }
        if (data.hasNextRecord())
            return true;
        else {
            noNext = true;
            return false;
        }
    }

    private List<Pair<Path, String>> getAggrePair() {
        List<Pair<Path, String>> aggres = new ArrayList<>();
        for(int i = 0; i < paths.size(); i++) {
            if(paths.size() == aggregations.size()) {
                aggres.add(new Pair<>(paths.get(i), aggregations.get(i)));
            } else {
                aggres.add(new Pair<>(paths.get(i), aggregations.get(0)));
            }
        }
        return aggres;
    }

    @Override
    public QueryDataSet next() {
        usedData = data;
        data = null;
        return usedData;
    }
}