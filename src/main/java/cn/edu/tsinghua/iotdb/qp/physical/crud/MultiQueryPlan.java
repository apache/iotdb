package cn.edu.tsinghua.iotdb.qp.physical.crud;

import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code MultiQueryPlan} is used in multi-pass SingleQueryPlan. Multi-pass means it's a disjunction
 * among a list of SeriesSelectPlans. {@code MultiQueryPlan} return a {@code Iterator<QueryDataSet>}
 * provided by {@code SingleQueryPlan} for one-pass SingleQueryPlan, or a {@code MergeQuerySetIterator} for
 * multi-pass SeriesSelectPlans.
 *
 * @see SingleQueryPlan
 * @author kangrong
 * @author qiaojialin
 */
public class MultiQueryPlan extends PhysicalPlan {
    private static Logger LOG = LoggerFactory.getLogger(MultiQueryPlan.class);
    private List<SingleQueryPlan> singleQueryPlans;
    private List<String> aggregations = new ArrayList<>();
    private long unit;
    private long origin;
    private List<Pair<Long, Long>> intervals;

    public QueryType getType() {
        return type;
    }

    public void setType(QueryType type) {
        this.type = type;
    }

    private QueryType type;

    public List<SingleQueryPlan> getSingleQueryPlans() {
        return singleQueryPlans;
    }

    public long getUnit() {
        return unit;
    }

    public void setUnit(long unit) {
        this.unit = unit;
    }

    public long getOrigin() {
        return origin;
    }

    public void setOrigin(long origin) {
        this.origin = origin;
    }

    public List<Pair<Long, Long>> getIntervals() {
        return intervals;
    }

    public void setIntervals(List<Pair<Long, Long>> intervals) {
        this.intervals = intervals;
    }

    public MultiQueryPlan(ArrayList<SingleQueryPlan> selectPlans, List<String> aggregations) {
        super(true, Operator.OperatorType.MERGEQUERY);
        if (selectPlans == null || selectPlans.isEmpty()) {
            LOG.error("cannot input an null or empty plan list into QuerySetMergePlan! ");
        }
        if (aggregations != null && !aggregations.isEmpty())
            setType(QueryType.AGGREGATION);
        else
            setType(QueryType.QUERY);
        this.singleQueryPlans = selectPlans;
        this.aggregations = aggregations;

    }

    public List<String> getAggregations() {
        return aggregations;
    }

    @Override
    public String printQueryPlan() {
        StringContainer sc = new StringContainer("\n");
        sc.addTail("MultiQueryPlan:");
        for (SingleQueryPlan singleQueryPlan : singleQueryPlans) {
            sc.addTail(singleQueryPlan.printQueryPlan());
        }
        return sc.toString();
    }

    @Override
    public List<Path> getPaths() {
        if(singleQueryPlans == null || singleQueryPlans.size() == 0)
            return new ArrayList<>();
        else{
            return singleQueryPlans.get(0).getPaths();

        }
    }

    public enum QueryType {
        QUERY, AGGREGATION, GROUPBY
    }
}
