package org.apache.iotdb.tsfile.read.filter.operator;

import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;

/**
 * Less than or Equals
 *
 * @param <T> comparable data type
 */
public class LtEq<T extends Comparable<T>> extends UnaryFilter<T> {

    private static final long serialVersionUID = -2088181659871608986L;

    public LtEq(T value, FilterType filterType) {
        super(value, filterType);
    }

    @Override
    public boolean satisfy(DigestForFilter digest) {
        if (filterType == FilterType.TIME_FILTER) {
            return ((Long) value) >= digest.getMinTime();
        } else {
            return value.compareTo(digest.getMinValue()) >= 0;
        }
    }

    @Override
    public boolean satisfy(long time, Object value) {
        Object v = filterType == FilterType.TIME_FILTER ? time : value;
        return this.value.compareTo((T) v) >= 0;
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
        if (filterType == FilterType.TIME_FILTER) {
            long time = (Long) value;
            if (time < startTime) {
                return false;
            }
            return true;
        } else {
            return true;
        }
    }

    @Override
    public String toString() {
        return getFilterType() + " <= " + value;
    }
}
