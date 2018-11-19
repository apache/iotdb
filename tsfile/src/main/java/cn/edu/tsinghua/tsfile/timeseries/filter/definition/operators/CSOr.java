package cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.FilterVisitor;

/**
 * Either of the left and right operators of CSOr must satisfy the condition
 * CSOr represents Cross Series Or operation.
 *
 * @author CGF
 */
public class CSOr extends CrossSeriesFilterExpression {
    public CSOr(FilterExpression left, FilterExpression right) {
        super(left, right);
    }

    public String toString() {
        return "[" + super.left + "]" + " | [" + super.right + "]";
    }

    /**
     * Not Used
     *
     * @param visitor filter visitor
     * @return accept filter
     */
    @Override
    public <T> T accept(FilterVisitor<T> visitor) {
        return null;
    }

    /**
     * Not Used
     *
     * @return filter
     */
    @Override
    public FilterSeries<?> getFilterSeries() {
        return null;
    }
}