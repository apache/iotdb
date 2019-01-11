package cn.edu.tsinghua.tsfile.read.expression.util;

import cn.edu.tsinghua.tsfile.read.expression.*;
import cn.edu.tsinghua.tsfile.read.expression.impl.GlobalTimeExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.BinaryExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.exception.filter.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.IUnaryExpression;
import cn.edu.tsinghua.tsfile.read.filter.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.read.common.Path;

import java.util.List;

public class ExpressionOptimizer {

    private static class QueryFilterOptimizerHelper {
        private static final ExpressionOptimizer INSTANCE = new ExpressionOptimizer();
    }

    private ExpressionOptimizer() {

    }

    /**
     * try to remove GlobalTimeExpression
     *
     * @param expression IExpression to be transferred
     * @param selectedSeries selected series
     * @return an executable query filter, whether a GlobalTimeExpression or All leaf nodes are SingleSeriesExpression
     */
    public IExpression optimize(IExpression expression, List<Path> selectedSeries) throws QueryFilterOptimizationException {
        if (expression instanceof IUnaryExpression) {
            return expression;
        } else if (expression instanceof IBinaryExpression) {
            ExpressionType relation = expression.getType();
            IExpression left = ((IBinaryExpression) expression).getLeft();
            IExpression right = ((IBinaryExpression) expression).getRight();
            if (left.getType() == ExpressionType.GLOBAL_TIME && right.getType() == ExpressionType.GLOBAL_TIME) {
                return combineTwoGlobalTimeFilter((GlobalTimeExpression) left, (GlobalTimeExpression) right, expression.getType());
            } else if (left.getType() == ExpressionType.GLOBAL_TIME && right.getType() != ExpressionType.GLOBAL_TIME) {
                return handleOneGlobalTimeFilter((GlobalTimeExpression) left, right, selectedSeries, relation);
            } else if (left.getType() != ExpressionType.GLOBAL_TIME && right.getType() == ExpressionType.GLOBAL_TIME) {
                return handleOneGlobalTimeFilter((GlobalTimeExpression) right, left, selectedSeries, relation);
            } else if (left.getType() != ExpressionType.GLOBAL_TIME && right.getType() != ExpressionType.GLOBAL_TIME) {
                IExpression regularLeft = optimize(left, selectedSeries);
                IExpression regularRight = optimize(right, selectedSeries);
                IBinaryExpression midRet = null;
                if (relation == ExpressionType.AND) {
                    midRet = BinaryExpression.and(regularLeft, regularRight);
                } else if (relation == ExpressionType.OR) {
                    midRet = BinaryExpression.or(regularLeft, regularRight);
                } else {
                    throw new UnsupportedOperationException("unsupported IExpression type: " + relation);
                }
                if (midRet.getLeft().getType() == ExpressionType.GLOBAL_TIME || midRet.getRight().getType() == ExpressionType.GLOBAL_TIME) {
                    return optimize(midRet, selectedSeries);
                } else {
                    return midRet;
                }

            } else if (left.getType() == ExpressionType.SERIES && right.getType() == ExpressionType.SERIES) {
                return expression;
            }
        }
        throw new UnsupportedOperationException("unknown IExpression type: " + expression.getClass().getName());
    }

    private IExpression handleOneGlobalTimeFilter(GlobalTimeExpression globalTimeExpression, IExpression expression
            , List<Path> selectedSeries, ExpressionType relation) throws QueryFilterOptimizationException {
        IExpression regularRightIExpression = optimize(expression, selectedSeries);
        if (regularRightIExpression instanceof GlobalTimeExpression) {
            return combineTwoGlobalTimeFilter(globalTimeExpression, (GlobalTimeExpression) regularRightIExpression, relation);
        }
        if (relation == ExpressionType.AND) {
            addTimeFilterToQueryFilter((globalTimeExpression).getFilter(), regularRightIExpression);
            return regularRightIExpression;
        } else if (relation == ExpressionType.OR) {
            return BinaryExpression.or(pushGlobalTimeFilterToAllSeries(globalTimeExpression, selectedSeries), regularRightIExpression);
        }
        throw new QueryFilterOptimizationException("unknown relation in IExpression:" + relation);
    }


    /**
     * Combine GlobalTimeExpression with all selected series
     *
     * example:
     *
     * input:
     *
     * GlobalTimeExpression(timeFilter)
     * Selected Series: path1, path2, path3
     *
     * output:
     *
     * QueryFilterOR(
     *      QueryFilterOR(
     *              SingleSeriesExpression(path1, timeFilter),
     *              SingleSeriesExpression(path2, timeFilter)
     *              ),
     *      SingleSeriesExpression(path3, timeFilter)
     * )
     *
     * @return a DNF query filter without GlobalTimeExpression
     */
    private IExpression pushGlobalTimeFilterToAllSeries(
            GlobalTimeExpression timeFilter, List<Path> selectedSeries) throws QueryFilterOptimizationException {
        if (selectedSeries.size() == 0) {
            throw new QueryFilterOptimizationException("size of selectSeries could not be 0");
        }
        IExpression IExpression = new SingleSeriesExpression(selectedSeries.get(0), timeFilter.getFilter());
        for (int i = 1; i < selectedSeries.size(); i++) {
            IExpression = BinaryExpression.or(IExpression, new SingleSeriesExpression(selectedSeries.get(i), timeFilter.getFilter()));
        }
        return IExpression;
    }


    /**
     * Combine TimeFilter with all SeriesFilters in the expression
     */
    private void addTimeFilterToQueryFilter(Filter timeFilter, IExpression expression) {
        if (expression instanceof SingleSeriesExpression) {
            addTimeFilterToSeriesFilter(timeFilter, (SingleSeriesExpression) expression);
        } else if (expression instanceof BinaryExpression) {
            addTimeFilterToQueryFilter(timeFilter, ((BinaryExpression) expression).getLeft());
            addTimeFilterToQueryFilter(timeFilter, ((BinaryExpression) expression).getRight());
        } else {
            throw new UnsupportedOperationException("IExpression should contains only SingleSeriesExpression but other type is found:"
                    + expression.getClass().getName());
        }
    }


    /**
     * Merge the timeFilter with the filter in SingleSeriesExpression with AndExpression
     *
     * example:
     *
     * input:
     *
     * timeFilter
     * SingleSeriesExpression(path, filter)
     *
     * output:
     *
     * SingleSeriesExpression(
     *      path,
     *      AndExpression(filter, timeFilter)
     *      )
     *
     */
    private void addTimeFilterToSeriesFilter(Filter timeFilter, SingleSeriesExpression singleSeriesExp) {
        singleSeriesExp.setFilter(FilterFactory.and(singleSeriesExp.getFilter(), timeFilter));
    }


    /**
     * combine two GlobalTimeExpression by merge the TimeFilter in each GlobalTimeExpression
     *
     * example:
     *
     * input:
     * QueryFilterAnd/OR(
     *      GlobalTimeExpression(timeFilter1),
     *      GlobalTimeExpression(timeFilter2)
     *      )
     *
     * output:
     *
     * GlobalTimeExpression(
     *      AndExpression/OR(timeFilter1, timeFilter2)
     *      )
     *
     */
    private GlobalTimeExpression combineTwoGlobalTimeFilter(GlobalTimeExpression left, GlobalTimeExpression right, ExpressionType type) {
        if (type == ExpressionType.AND) {
            return new GlobalTimeExpression(FilterFactory.and(left.getFilter(), right.getFilter()));
        } else if (type == ExpressionType.OR) {
            return new GlobalTimeExpression(FilterFactory.or(left.getFilter(), right.getFilter()));
        }
        throw new UnsupportedOperationException("unrecognized QueryFilterOperatorType :" + type);
    }

    public static ExpressionOptimizer getInstance() {
        return QueryFilterOptimizerHelper.INSTANCE;
    }
}
