package cn.edu.thu.tsfiledb.qp.physical.crud;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;

/**
 * {@code MergeQuerySetPlan} is used in multi-pass SeriesSelectPlan. Multi-pass means it's a disjunction
 * among a list of SeriesSelectPlans. {@code MergeQuerySetPlan} return a {@code Iterator<QueryDataSet>}
 * provided by {@code SeriesSelectPlan} for one-pass SeriesSelectPlan, or a {@code MergeQuerySetIterator} for
 * multi-pass SeriesSelectPlans.
 * 
 * @see SeriesSelectPlan
 * @author kangrong
 *
 */
public class MergeQuerySetPlan extends PhysicalPlan {
    private static Logger LOG = LoggerFactory.getLogger(MergeQuerySetPlan.class);
    private List<SeriesSelectPlan> seriesSelectPlans;
    
    public List<SeriesSelectPlan> getSeriesSelectPlans() {
        return seriesSelectPlans;
    }

    public MergeQuerySetPlan(ArrayList<SeriesSelectPlan> selectPlans) {
        super(true, OperatorType.MERGEQUERY);
        if (selectPlans == null || selectPlans.isEmpty()) {
            LOG.error("cannot input an null or empty plan list into QuerySetMergePlan! ");
        }
        this.seriesSelectPlans = selectPlans;
    }

    public void setSeriesSelectPlans(List<SeriesSelectPlan> seriesSelectPlans) {
        this.seriesSelectPlans = seriesSelectPlans;
    }

    @Override
    public Iterator<QueryDataSet> processQuery(QueryProcessExecutor executor) throws QueryProcessorException {
        if (seriesSelectPlans.size() == 1)
            return executor.processQuery(seriesSelectPlans.get(0));
        else
            return new MergeQuerySetIterator(seriesSelectPlans, executor.getFetchSize(), executor);
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
         	LinkedHashMap<Path,Integer> pathMap = new LinkedHashMap<>();
            for (SeriesSelectPlan series : seriesSelectPlans) {
            	for(Path p : series.getPaths()){
            		if(!pathMap.containsKey(p)){
            			pathMap.put(p, 1);
            			ret.add(p);
            		}
            	}
            }
            return ret;
        }
    }
}
