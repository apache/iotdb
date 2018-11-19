package cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.*;

/**
 * To judge whether an overflow time interval satisfy the filter.
 * Implemented using visitor pattern.
 *
 * @author CGF
 */
public class IntervalTimeVisitor implements FilterVisitor<Boolean> {

    private Long startTime, endTime;

    public boolean satisfy(SingleSeriesFilterExpression timeFilter, Long s, Long e) {
        if (timeFilter == null) {
            return true;
        }

        this.startTime = s;
        this.endTime = e;
        return timeFilter.accept(this);
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
        return (Long) eq.getValue() >= startTime && (Long) eq.getValue() <= endTime;

    }

    @Override
    public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
        if (startTime.equals(endTime) && (notEq.getValue()).equals(startTime)) {
            return false;
        }
        return true;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
        if (ltEq.getIfEq()) {
            return (Long) ltEq.getValue() >= startTime;
        } else {
            return (Long) ltEq.getValue() > startTime;
        }
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
        if (gtEq.getIfEq()) {
            return (Long) gtEq.getValue() <= endTime;
        } else {
            return (Long) gtEq.getValue() < endTime;
        }
    }

    @Override
    public Boolean visit(Not not) {
        return !satisfy(not.getFilterExpression(), startTime, endTime);
    }

    @Override
    public Boolean visit(And and) {
        return satisfy(and.getLeft(), startTime, endTime) && satisfy(and.getRight(), startTime, endTime);
    }

    @Override
    public Boolean visit(Or or) {
        return satisfy(or.getLeft(), startTime, endTime) || satisfy(or.getRight(), startTime, endTime);
    }

    @Override
    public Boolean visit(NoFilter noFilter) {
        return true;
    }

}
