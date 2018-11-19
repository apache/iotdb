package cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

/**
 * Not Equals
 *
 * @param <T> comparable data type
 * @author CGF
 */
public class NotEq<T extends Comparable<T>> extends SingleUnaryExpression<T> {

    private static final long serialVersionUID = 2574090797476500965L;

    public NotEq(FilterSeries<T> filterSeries, T value) {
        super(filterSeries, value);
    }

    @Override
    public <R> R accept(FilterVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return filterSeries + " != " + value;
    }
}
