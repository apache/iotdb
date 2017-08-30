package cn.edu.thu.tsfiledb.qp.physical.crud;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;

/**
 * {@code MergeQuerySetPlan} is used in multi-pass SeriesSelectPlan. Multi-pass means it's a disjunction
 * among a list of SeriesSelectPlans. {@code MergeQuerySetPlan} return a {@code Iterator<QueryDataSet>}
 * provided by {@code SeriesSelectPlan} for one-pass SeriesSelectPlan, or a {@code MergeQuerySetIterator} for
 * multi-pass SeriesSelectPlans.
 *
 * @see SeriesSelectPlan
 * @author kangrong
 * @author qiaojialin
 */
public class MergeQuerySetPlan extends PhysicalPlan {
    private static Logger LOG = LoggerFactory.getLogger(MergeQuerySetPlan.class);
    private List<SeriesSelectPlan> seriesSelectPlans;
    private List<String> aggregations = new ArrayList<>();

    public List<SeriesSelectPlan> getSeriesSelectPlans() {
        return seriesSelectPlans;
    }

    public MergeQuerySetPlan(ArrayList<SeriesSelectPlan> selectPlans, List<String> aggregations) {
        super(true, OperatorType.MERGEQUERY);
        if (selectPlans == null || selectPlans.isEmpty()) {
            LOG.error("cannot input an null or empty plan list into QuerySetMergePlan! ");
        }
        this.seriesSelectPlans = selectPlans;
        this.aggregations = aggregations;
    }

    public List<String> getAggregations() {
        return aggregations;
    }

    @Override
    public String printQueryPlan() {
        StringContainer sc = new StringContainer("\n");
        for (int i = 0; i < seriesSelectPlans.size(); i++) {
            sc.addTail("showing series plan:" + i);
            sc.addTail(seriesSelectPlans.get(i).printQueryPlan());
        }
        return sc.toString();
    }

    @Override
    public List<Path> getPaths() {
        if(seriesSelectPlans == null || seriesSelectPlans.size() == 0)
            return new ArrayList<>();
        else{
            List<Path> ret = new ArrayList<>();
            HashSet<Path> pathMap = new HashSet<>();
            for (SeriesSelectPlan series : seriesSelectPlans) {
                for(Path p : series.getPaths()){
                    if(!pathMap.contains(p)){
                        pathMap.add(p);
                        ret.add(p);
                    }
                }
            }
            return ret;
        }
    }
}
