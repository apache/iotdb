package cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

/**
 * <code>NoFilter</code> means that there is no filter.
 */
public class NoFilter extends SingleSeriesFilterExpression{
    private static NoFilter noFilter;

    private static class SingletonHolder {
        private static final NoFilter INSTANCE = new NoFilter();
    }

    private NoFilter() {
    }

    public static final NoFilter getInstance() {
        return SingletonHolder.INSTANCE;
    }

    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return null;
    }

    @Override
    public FilterSeries<?> getFilterSeries() {
        return null;
    }
}
