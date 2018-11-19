package cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.util;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.exception.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.BinaryQueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.UnaryQueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/19.
 */
public class QueryFilterOptimizer {
    
    private static class QueryFilterOptimizerHelper {
        private static final QueryFilterOptimizer INSTANCE = new QueryFilterOptimizer();
    }

    private QueryFilterOptimizer() {

    }

    public QueryFilter convertGlobalTimeFilter(QueryFilter queryFilter, List<Path> selectedSeries) throws QueryFilterOptimizationException {
        if (queryFilter instanceof UnaryQueryFilter) {
            return queryFilter;
        } else if (queryFilter instanceof BinaryQueryFilter) {
            QueryFilterType relation = queryFilter.getType();
            QueryFilter left = ((BinaryQueryFilter) queryFilter).getLeft();
            QueryFilter right = ((BinaryQueryFilter) queryFilter).getRight();
            if (left.getType() == QueryFilterType.GLOBAL_TIME && right.getType() == QueryFilterType.GLOBAL_TIME) {
                return combineTwoGlobalTimeFilter((GlobalTimeFilter) left, (GlobalTimeFilter) right, queryFilter.getType());
            } else if (left.getType() == QueryFilterType.GLOBAL_TIME && right.getType() != QueryFilterType.GLOBAL_TIME) {
                return handleOneGlobalTimeFilter((GlobalTimeFilter) left, right, selectedSeries, relation);
            } else if (left.getType() != QueryFilterType.GLOBAL_TIME && right.getType() == QueryFilterType.GLOBAL_TIME) {
                return handleOneGlobalTimeFilter((GlobalTimeFilter) right, left, selectedSeries, relation);
            } else if (left.getType() != QueryFilterType.GLOBAL_TIME && right.getType() != QueryFilterType.GLOBAL_TIME) {
                QueryFilter regularLeft = convertGlobalTimeFilter(left, selectedSeries);
                QueryFilter regularRight = convertGlobalTimeFilter(right, selectedSeries);
                BinaryQueryFilter midRet = null;
                if (relation == QueryFilterType.AND) {
                    midRet = QueryFilterFactory.and(regularLeft, regularRight);
                } else if (relation == QueryFilterType.OR) {
                    midRet = QueryFilterFactory.or(regularLeft, regularRight);
                } else {
                    throw new UnsupportedOperationException("unsupported queryFilter type: " + relation);
                }
                if (midRet.getLeft().getType() == QueryFilterType.GLOBAL_TIME || midRet.getRight().getType() == QueryFilterType.GLOBAL_TIME) {
                    return convertGlobalTimeFilter(midRet, selectedSeries);
                } else {
                    return midRet;
                }

            } else if (left.getType() == QueryFilterType.SERIES && right.getType() == QueryFilterType.SERIES) {
                return queryFilter;
            }
        }
        throw new UnsupportedOperationException("unknown queryFilter type: " + queryFilter.getClass().getName());
    }

    private QueryFilter handleOneGlobalTimeFilter(GlobalTimeFilter globalTimeFilter, QueryFilter queryFilter
            , List<Path> selectedSeries, QueryFilterType relation) throws QueryFilterOptimizationException {
        QueryFilter regularRightQueryFilter = convertGlobalTimeFilter(queryFilter, selectedSeries);
        if (regularRightQueryFilter instanceof GlobalTimeFilter) {
            return combineTwoGlobalTimeFilter(globalTimeFilter, (GlobalTimeFilter) regularRightQueryFilter, relation);
        }
        if (relation == QueryFilterType.AND) {
            addTimeFilterToQueryFilter((globalTimeFilter).getFilter(), regularRightQueryFilter);
            return regularRightQueryFilter;
        } else if (relation == QueryFilterType.OR) {
            return QueryFilterFactory.or(convertGlobalTimeFilterToQueryFilterBySeriesList(globalTimeFilter, selectedSeries), queryFilter);
        }
        throw new QueryFilterOptimizationException("unknown relation in queryFilter:" + relation);
    }

    private QueryFilter convertGlobalTimeFilterToQueryFilterBySeriesList(
            GlobalTimeFilter timeFilter, List<Path> selectedSeries) throws QueryFilterOptimizationException {
        if (selectedSeries.size() == 0) {
            throw new QueryFilterOptimizationException("size of selectSeries could not be 0");
        }
        SeriesFilter firstSeriesFilter = new SeriesFilter(selectedSeries.get(0), timeFilter.getFilter());
        QueryFilter queryFilter = firstSeriesFilter;
        for (int i = 1; i < selectedSeries.size(); i++) {
            queryFilter = QueryFilterFactory.or(queryFilter, new SeriesFilter(selectedSeries.get(i), timeFilter.getFilter()));
        }
        return queryFilter;
    }

    private void addTimeFilterToQueryFilter(Filter<?> timeFilter, QueryFilter queryFilter) {
        if (queryFilter instanceof SeriesFilter) {
            addTimeFilterToSeriesFilter(timeFilter, (SeriesFilter) queryFilter);
        } else if (queryFilter instanceof QueryFilterFactory) {
            addTimeFilterToQueryFilter(timeFilter, ((QueryFilterFactory) queryFilter).getLeft());
            addTimeFilterToQueryFilter(timeFilter, ((QueryFilterFactory) queryFilter).getRight());
        } else {
            throw new UnsupportedOperationException("queryFilter should contains only SeriesFilter but other type is found:"
                    + queryFilter.getClass().getName());
        }
    }

    private void addTimeFilterToSeriesFilter(Filter<?> timeFilter, SeriesFilter seriesFilter) {
        seriesFilter.setFilter(FilterFactory.and(seriesFilter.getFilter(), timeFilter));
    }

    private GlobalTimeFilter combineTwoGlobalTimeFilter(GlobalTimeFilter left, GlobalTimeFilter right, QueryFilterType type) {
        if (type == QueryFilterType.AND) {
            return new GlobalTimeFilter(FilterFactory.and(left.getFilter(), right.getFilter()));
        } else if (type == QueryFilterType.OR) {
            return new GlobalTimeFilter(FilterFactory.or(left.getFilter(), right.getFilter()));
        }
        throw new UnsupportedOperationException("unrecognized QueryFilterOperatorType :" + type);
    }

    public static QueryFilterOptimizer getInstance() {
        return QueryFilterOptimizerHelper.INSTANCE;
    }
}
