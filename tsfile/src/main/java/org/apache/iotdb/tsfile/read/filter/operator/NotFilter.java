package org.apache.iotdb.tsfile.read.filter.operator;

import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterType;

import java.io.Serializable;

/**
 * NotFilter necessary. Use InvertExpressionVisitor
 */
public class NotFilter implements Filter, Serializable {

    private static final long serialVersionUID = 584860326604020881L;
    private Filter that;

    public NotFilter(Filter that) {
        this.that = that;
    }

    @Override
    public boolean satisfy(DigestForFilter digest) {
        return !that.satisfy(digest);
    }

    @Override
    public boolean satisfy(long time, Object value) {
        return !that.satisfy(time, value);
    }

    /**
     * Notice that, if the not filter only contains value filter, this method may return false,
     * this may cause misunderstanding.
     */
    @Override
    public boolean satisfyStartEndTime(long startTime, long endTime) {
        return !that.satisfyStartEndTime(startTime, endTime);
    }

    public Filter getFilter() {
        return this.that;
    }

    @Override
    public String toString() {
        return "NotFilter: " + that;
    }

}
