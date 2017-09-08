package cn.edu.thu.tsfiledb.qp.executor.iterator;

import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.query.engine.FilterStructure;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;

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

    //single query and single aggregations
    public QueryDataSetIterator(List<Path> paths, int fetchSize, QueryProcessExecutor executor,
                                FilterExpression timeFilter, FilterExpression freqFilter,
                                FilterExpression valueFilter, List<String> aggregations) {
        this.paths = paths;
        this.fetchSize = fetchSize;
        this.executor = executor;
        this.filterStructures.add(new FilterStructure(timeFilter, freqFilter, valueFilter));
        this.aggregations = aggregations;
    }

    //merge aggregations
    public QueryDataSetIterator(List<Path> paths, int fetchSize, List<String> aggregations,
                                List<FilterStructure> filterStructures, QueryProcessExecutor executor) {
        this.fetchSize = fetchSize;
        this.executor = executor;
        this.filterStructures = filterStructures;
        this.paths = paths;
        this.aggregations = aggregations;
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
                //aggregations
                if(aggregations != null && !aggregations.isEmpty()) {
                    List<Pair<Path, String>> aggres = new ArrayList<>();
                    for(int i = 0; i < paths.size(); i++) {
                        if(paths.size() == aggregations.size()) {
                            aggres.add(new Pair<>(paths.get(i), aggregations.get(i)));
                        } else {
                            aggres.add(new Pair<>(paths.get(i), aggregations.get(0)));
                        }
                    }
                    data = executor.aggregate(aggres, filterStructures);
                } else {
                    //query
                    FilterStructure filterStructure = filterStructures.get(0);
                    data = executor.query(0, paths, filterStructure.getTimeFilter(), filterStructure.getFrequencyFilter(),
                            filterStructure.getValueFilter(), fetchSize, usedData);
                }
            } catch (Exception e) {
                throw new RuntimeException("meet error in hasNext" + Arrays.toString(e.getStackTrace()));
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

    @Override
    public QueryDataSet next() {
        usedData = data;
        data = null;
        return usedData;
    }
}