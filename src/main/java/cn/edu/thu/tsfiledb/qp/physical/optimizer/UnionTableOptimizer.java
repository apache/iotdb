package cn.edu.thu.tsfiledb.qp.physical.optimizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.constant.QueryConstant;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.logical.optimize.MergeFilterException;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.BasicFunctionOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.crud.FilterOperator;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.query.MergeQuerySetPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.query.SeriesSelectPlan;



/**
 * This class deals with delta object in union table
 * 
 * @author kangrong
 *
 */
public class UnionTableOptimizer implements IPhysicalOptimizer {
    private static final Logger LOG = LoggerFactory.getLogger(UnionTableOptimizer.class);
    private MergeQuerySetPlan plan;

    @Override
    public PhysicalPlan transform(PhysicalPlan plan, QueryProcessExecutor conf) {
        MergeQuerySetPlan mergePlan = (MergeQuerySetPlan) plan;
        ArrayList<String> alldeltaObjects = (ArrayList<String>)conf.getParameter(QueryConstant.ALL_DEVICES);

        for (SeriesSelectPlan seriesPlan : mergePlan.getSelectPlans()) {
            List<SeriesSelectPlan> newPlans = new ArrayList<>();
            Set<String> selectDeltaObjects = mergeDeltaObject(seriesPlan.getDeltaObjectFilterOperator());

            ArrayList<String> actualDeltaObjects = new ArrayList<>();
            //if select deltaObject, then match with measurement
            if(selectDeltaObjects != null && selectDeltaObjects.size() >= 1) {
                actualDeltaObjects.addAll(selectDeltaObjects);
            } else {
                actualDeltaObjects.addAll(alldeltaObjects);
            }

            FilterOperator timeFilter = seriesPlan.getTimeFilterOperator();
            FilterOperator freqFilter = seriesPlan.getFreqFilterOperator();
            FilterOperator valueFilter = seriesPlan.getValueFilterOperator();
            List<Path> paths = seriesPlan.getPaths();

            for(String deltaObject: actualDeltaObjects) {
                List<Path> newPaths = new ArrayList<>();
                for(Path path: paths) {
                    Path newPath = new Path(deltaObject + "." + path.getFullPath());
                    newPaths.add(newPath);
                }
                SeriesSelectPlan newSeriesPlan;
                if(valueFilter == null) {
                    newSeriesPlan = new SeriesSelectPlan(newPaths, timeFilter, freqFilter, null,conf);
                } else {
                    FilterOperator newValueFilter = valueFilter.clone();
                    newValueFilter.addHeadDeltaObjectPath(deltaObject);
                    newSeriesPlan = new SeriesSelectPlan(newPaths, timeFilter, freqFilter, newValueFilter,conf);
                }
                newPlans.add(newSeriesPlan);
            }
            mergePlan.setSelectPlans(newPlans);
        }
        return mergePlan;
    }

    public Set<String> mergeDeltaObject(FilterOperator deltaFilterOperator) {
        if (deltaFilterOperator == null) {
            System.out.println("deltaFilterOperator is null");
            return null;
        }
        if (deltaFilterOperator.isLeaf()) {
            Set<String> r = new HashSet<String>();
            r.add(((BasicFunctionOperator)deltaFilterOperator).getSeriesValue());
            return r;
        }
        List<FilterOperator> children = deltaFilterOperator.getChildren();
        if (children.isEmpty()) {
            return new HashSet<String>();
        }
        Set<String> ret = mergeDeltaObject(children.get(0));
        for (int i = 1; i < children.size(); i++) {
            Set<String> temp = mergeDeltaObject(children.get(i));
            switch (deltaFilterOperator.getTokenIntType()) {
                case SQLConstant.KW_AND:
                    ret.retainAll(temp);
                    break;
                case SQLConstant.KW_OR:
                    ret.addAll(temp);
                    break;
                default:
                    throw new UnsupportedOperationException("given error token type:"+deltaFilterOperator.getTokenIntType());
            }
        }
        return ret;
    }
}
