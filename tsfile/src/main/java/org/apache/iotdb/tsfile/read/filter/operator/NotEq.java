package org.apache.iotdb.tsfile.read.filter.operator;


import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;

/**
 * NotFilter Equals
 *
 * @param <T> comparable data type
 */
public class NotEq<T extends Comparable<T>> extends UnaryFilter<T> {

    private static final long serialVersionUID = 2574090797476500965L;

    public NotEq(T value, FilterType filterType) {
        super(value, filterType);
    }

    @Override
    public boolean satisfy(DigestForFilter digest) {
        if (filterType == FilterType.TIME_FILTER) {
            return !(((Long) value) == digest.getMinTime()
                    && (Long) value == digest.getMaxTime());
        } else {
            return !(value.compareTo(digest.getMinValue()) == 0
                    && value.compareTo(digest.getMaxValue()) == 0);
        }
    }

    @Override
    public boolean satisfy(long time, Object value) {
        Object v = filterType == FilterType.TIME_FILTER ? time : value;
        return !this.value.equals(v);
    }

    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
        if (filterType == FilterType.TIME_FILTER) {
            long time = (Long) value;
            if (time == startTime && time == endTime) {
                return false;
            }
            return true;
        } else {
            return true;
        }
    }

    @Override
    public String toString() {
        return getFilterType() + " != " + value;
    }
}
