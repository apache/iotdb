package cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

/**
 * Greater than | Equals
 *
 * @param <T> comparable data type
 * @author CGF
 */
public class GtEq<T extends Comparable<T>> extends SingleUnaryExpression<T> {

    private static final long serialVersionUID = -2088181659871608986L;
    public boolean ifEq = false;

    public GtEq(FilterSeries<T> filterSeries, T value, Boolean ifEq) {
        super(filterSeries, value);
        this.ifEq = ifEq;
    }

    @Override
    public <R> R accept(FilterVisitor<R> visitor) {
        return visitor.visit(this);
    }

    public Boolean getIfEq() {
        return this.ifEq;
    }

    @Override
    public String toString() {
        if (ifEq)
            return filterSeries + " >= " + value;
        else
            return filterSeries + " > " + value;
    }
}
