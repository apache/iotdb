package cn.edu.thu.tsfiledb.qp.physical.plan.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

/**
 * {@code MergeQuerySetPlan} is used in multi-pass getIndex plan. Multi-pass means it's a disjunction
 * among a list of single getIndex. {@code MergeQuerySetPlan} return a {@code Iterator<QueryDataSet>}
 * provided by {@code SeriesSelectPlan} for one-pass getIndex, or a {@code MergeQuerySetIterator} for
 * multi-pass getIndex.
 * 
 * @see cn.edu.thu.tsfiledb.qp.physical.plan.query.SeriesSelectPlan
 * @author kangrong
 *
 */
public class MergeQuerySetPlan extends PhysicalPlan {
    private static Logger LOG = LoggerFactory.getLogger(MergeQuerySetPlan.class);
    protected List<SeriesSelectPlan> selectPlans;
    
    public List<SeriesSelectPlan> getSelectPlans() {
        return selectPlans;
    }

    public MergeQuerySetPlan(ArrayList<SeriesSelectPlan> selectPlans) {
        super(true, OperatorType.QUERY);
        if (selectPlans == null || selectPlans.isEmpty()) {
            LOG.error("cannot input an null or empty plan list into QuerySetMergePlan! ");
        }
        this.selectPlans = selectPlans;
    }

    public void setSelectPlans(List<SeriesSelectPlan> selectPlans) {
        this.selectPlans = selectPlans;
    }

    @Override
    public Iterator<QueryDataSet> processQuery(QueryProcessExecutor conf) {
        if (selectPlans.size() == 1)
            // return new SingleQuerySetIterator(conf, selectPlans[0]);
            return selectPlans.get(0).processQuery(conf);
        else
            return new MergeQuerySetIterator(selectPlans, conf.getFetchSize(), conf);
    }

    @Override
    public String printQueryPlan() {
        // LOG.info("show getIndex plan:");
        StringContainer sc = new StringContainer("\n");
        for (int i = 0; i < selectPlans.size(); i++) {
            sc.addTail("showing series plan:" + i);
            sc.addTail(selectPlans.get(i).printQueryPlan());
        }
        return sc.toString();
    }
    
    @Override
    public List<Path> getInvolvedSeriesPaths() {
        if(selectPlans == null || selectPlans.size() == 0)
            return new ArrayList<Path>();
        else{
        	List<Path> ret = new ArrayList<>();
         	LinkedHashMap<Path,Integer> pathMap = new LinkedHashMap<>();
            for (SeriesSelectPlan series : selectPlans) {
            	for(Path p : series.getInvolvedSeriesPaths()){
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
