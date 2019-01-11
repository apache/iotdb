package cn.edu.tsinghua.tsfile.read.filter.operator;

import cn.edu.tsinghua.tsfile.read.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.read.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.filter.basic.UnaryFilter;
import cn.edu.tsinghua.tsfile.read.filter.factory.FilterType;

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
