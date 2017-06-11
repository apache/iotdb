package cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw;

import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.KW_AND;
import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.KW_OR;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.qp.logical.operator.clause.filter.FilterOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.constant.SQLConstant;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QueryOperatorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.query.MergeQuerySetPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.query.SeriesSelectPlan;


/**
 * this class extends {@code RootOperator} and process getIndex statement
 * 
 */
public class QueryOperator extends SFWOperator {
    private static final Logger LOG = LoggerFactory.getLogger(QueryOperator.class);

    public QueryOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.QUERY;
    }

    @Override
    public PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor executor)
            throws QueryProcessorException {
        List<Path> paths = getSelSeriesPaths(executor);
        String aggregation = selectOperator.getAggregation();
        if(aggregation != null)
            executor.addParameter(SQLConstant.IS_AGGREGATION, aggregation);
        ArrayList<SeriesSelectPlan> subPlans = new ArrayList<>();
        if (filterOperator == null) {
            subPlans.add(new SeriesSelectPlan(paths, null, null, null, executor));
        }
        else{
            List<FilterOperator> parts = splitFilter();
            for (FilterOperator filterOperator : parts) {
                SeriesSelectPlan plan = constructSelectPlan(filterOperator, paths, executor);
                if (plan != null)
                    subPlans.add(plan);
            }
        }
        return new MergeQuerySetPlan(subPlans);
    }

    private SeriesSelectPlan constructSelectPlan(FilterOperator filterOperator, List<Path> paths,
            QueryProcessExecutor conf) throws QueryProcessorException {
        FilterOperator timeFilter = null;
        FilterOperator freqFilter = null;
        FilterOperator valueFilter = null;
        List<FilterOperator> singleFilterList;
        if (filterOperator.isSingle()) {
            singleFilterList = new ArrayList<>();
            singleFilterList.add(filterOperator);
        } else if (filterOperator.getTokenIntType() == KW_AND) {
            // now it has been dealt with merge optimizer, thus all nodes with same path have been
            // merged to one node
            singleFilterList = filterOperator.getChildren();
        } else {
            throw new QueryOperatorException(
                    "for one tasks, filter cannot be OR if it's not single");
        }
        List<FilterOperator> valueList = new ArrayList<>();
        for (FilterOperator child : singleFilterList) {
            if (!child.isSingle()) {
                throw new QueryOperatorException(
                        "in format:[(a) and () and ()] or [] or [], a is not single! a:" + child);
            }
            switch (child.getSinglePath().toString()) {
                case SQLConstant.RESERVED_TIME:
                    if (timeFilter != null) {
                        throw new QueryOperatorException(
                                "time filter has been specified more than once");
                    }
                    timeFilter = child;
                    break;
                case SQLConstant.RESERVED_FREQ:
                    if (freqFilter != null) {
                        throw new QueryOperatorException(
                                "freq filter has been specified more than once");
                    }
                    freqFilter = child;
                    break;
                default:
                    valueList.add(child);
                    break;
            }
        }
        if (valueList.size() == 1) {
            valueFilter = valueList.get(0);
        } else if (valueList.size() > 1) {
            valueFilter = new FilterOperator(KW_AND, false);
            valueFilter.setChildren(valueList);
        }
        
        return new SeriesSelectPlan(paths, timeFilter, freqFilter, valueFilter, conf);
    }

    /**
     * split filter operator to a list of filter with relation of "or" each other.
     *
     */
    private List<FilterOperator> splitFilter() {
        List<FilterOperator> ret = new ArrayList<>();
        if (filterOperator.isSingle() || filterOperator.getTokenIntType() != KW_OR) {
            // single or leaf(BasicFunction)
            ret.add(filterOperator);
            return ret;
        }
        // a list of partion linked with or
        return filterOperator.getChildren();
    }
}
