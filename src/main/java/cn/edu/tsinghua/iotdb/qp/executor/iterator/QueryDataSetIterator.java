package cn.edu.tsinghua.iotdb.qp.executor.iterator;

import cn.edu.tsinghua.iotdb.qp.executor.QueryProcessExecutor;
import cn.edu.tsinghua.iotdb.query.management.FilterStructure;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.read.query.OnePassQueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.OldRowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class QueryDataSetIterator implements QueryDataSet {

    private static final Logger logger = LoggerFactory.getLogger(QueryDataSetIterator.class);
    private final int fetchSize;
    private final QueryProcessExecutor executor;
    private OnePassQueryDataSet data = null;
    private List<Path> paths;
    private List<String> aggregations;
    private List<FilterStructure> filterStructures = new ArrayList<>();
    private boolean hasNext = true;

    //group by
    private long unit;
    private long origin;
    private List<Pair<Long, Long>> intervals;


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
    }


    @Override
    public boolean hasNext() {
        if(!hasNext)
            return false;
        if (data == null || !data.hasNextRecord()) {
            try {
                data = executor.groupBy(getAggrePair(), filterStructures, unit, origin, intervals, fetchSize);
                if(data.hasNextRecord()) {
                    return true;
                } else {
                    hasNext = false;
                    return false;
                }
            } catch (Exception e) {
                logger.error("meet error in hasNext because: "+ e.getMessage());
            }
        }
        return true;
    }

    private List<Pair<Path, String>> getAggrePair() {
        List<Pair<Path, String>> aggres = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            if (paths.size() == aggregations.size()) {
                aggres.add(new Pair<>(paths.get(i), aggregations.get(i)));
            } else {
                aggres.add(new Pair<>(paths.get(i), aggregations.get(0)));
            }
        }
        return aggres;
    }

    @Override
    public RowRecord next() {
        OldRowRecord oldRowRecord = data.getNextRecord();
        return OnePassQueryDataSet.convertToNew(oldRowRecord);
    }

}
