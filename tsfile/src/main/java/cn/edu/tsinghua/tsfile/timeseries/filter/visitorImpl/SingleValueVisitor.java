package cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.*;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.*;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.FilterVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To judge whether a single value satisfy the filter.
 * Implemented per visitor pattern.
 *
 * @param <V> data type for filter
 * @author CGF
 */
public class SingleValueVisitor<V extends Comparable<V>> implements FilterVisitor<Boolean> {

    private static final Logger LOG = LoggerFactory.getLogger(SingleValueVisitor.class);
    private V value;
    private FilterVerifier verifier;
    private SingleSeriesFilterExpression singleSeriesFilter;
    private Interval interval;

    public SingleValueVisitor() {
    }

    /**
     * This method is only used for INT32,INT64,FLOAT,DOUBLE data type.
     *
     * @param filter
     */
    public SingleValueVisitor(SingleSeriesFilterExpression filter) {
        verifier = FilterVerifier.create(filter.getFilterSeries().getSeriesDataType());
        this.singleSeriesFilter = filter;
        interval = verifier.getInterval(singleSeriesFilter);
    }

    /**
     * This method exits a problem, the data type of value must accord with filter.
     *
     * @param value value to filter
     * @param filter filter
     * @return is satisfied
     */
    public Boolean satisfyObject(Object value, SingleSeriesFilterExpression filter) {
        if (filter == null)
            return true;

        // The value type and filter type may not be consistent
        return this.satisfy((V) value, filter);
    }

    private Boolean satisfy(V value, SingleSeriesFilterExpression filter) {
        this.value = value;
        return filter.accept(this);
    }

    /**
     * optimization of filter, filter is value interval
     *
     * @param value value to filter
     * @return is satisfied
     */
    public boolean verify(int value) {
        IntInterval val = (IntInterval) interval;
        for (int i = 0; i < val.count; i += 2) {
            if (val.v[i] < value && value < val.v[i + 1])
                return true;
            if (val.v[i] == value && val.flag[i])
                return true;
            if (val.v[i + 1] == value && val.flag[i + 1])
                return true;
        }
        return false;
    }

    public boolean verify(long value) {
        LongInterval val = (LongInterval) interval;
        for (int i = 0; i < val.count; i += 2) {
            if (val.v[i] < value && value < val.v[i + 1])
                return true;
            if (val.v[i] == value && val.flag[i])
                return true;
            if (val.v[i + 1] == value && val.flag[i + 1])
                return true;
        }
        return false;
    }

    public boolean verify(float value) {
        FloatInterval val = (FloatInterval) interval;
        for (int i = 0; i < val.count; i += 2) {
            if (val.v[i] < value && value < val.v[i + 1])
                return true;
            if (val.v[i] == value && val.flag[i])
                return true;
            if (val.v[i + 1] == value && val.flag[i + 1])
                return true;
        }
        return false;
    }

    public boolean verify(double value) {
        DoubleInterval val = (DoubleInterval) interval;
        for (int i = 0; i < val.count; i += 2) {
            if (val.v[i] < value && value < val.v[i + 1])
                return true;
            if (val.v[i] == value && val.flag[i])
                return true;
            if (val.v[i + 1] == value && val.flag[i + 1])
                return true;
        }
        return false;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
        if (eq.getValue().equals(value))
            return true;
        return false;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
        if (!notEq.getValue().equals(value))
            return true;
        return false;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
        if (ltEq.getIfEq() && ltEq.getValue().compareTo((T) value) >= 0)
            return true;
        if (!ltEq.getIfEq() && ltEq.getValue().compareTo((T) value) > 0)
            return true;
        return false;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
        if (gtEq.getIfEq() && gtEq.getValue().compareTo((T) value) <= 0)
            return true;
        if (!gtEq.getIfEq() && gtEq.getValue().compareTo((T) value) < 0)
            return true;
        return false;
    }

    @Override
    public Boolean visit(Not not) {
        if (satisfy(value, not.getFilterExpression()))
            return false;
        return true;
    }

    @Override
    public Boolean visit(And and) {
        return satisfy(value, and.getLeft()) && satisfy(value, and.getRight());
    }

    @Override
    public Boolean visit(Or or) {
        return satisfy(value, or.getLeft()) || satisfy(value, or.getRight());
    }

    @Override
    public Boolean visit(NoFilter noFilter) {
        return true;
    }

    public Interval getInterval() {
        return this.interval;
    }
}
