package cn.edu.tsinghua.tsfile.timeseries.filterV2.basic;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterType;

import java.io.Serializable;

/**
 * Definition for unary filter operations
 *
 * @param <T> comparable data type
 * @author CGF
 */
public abstract class UnaryFilter<T extends Comparable<T>> implements Filter<T>, Serializable {

    private static final long serialVersionUID = 1431606024929453556L;
    protected final T value;

    protected FilterType filterType;

    protected UnaryFilter(T value, FilterType filterType) {
        this.value = value;
        this.filterType = filterType;
    }

    public T getValue() {
        return value;
    }

    public FilterType getFilterType() {
        return filterType;
    }

    @Override
    public abstract String toString();
}
