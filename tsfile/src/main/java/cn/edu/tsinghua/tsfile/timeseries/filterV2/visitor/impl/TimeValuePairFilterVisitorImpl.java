package cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter.*;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.operator.*;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.TimeValuePairFilterVisitor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

/**
 * Created by zhangjinrui on 2017/12/15.
 */
public class TimeValuePairFilterVisitorImpl implements TimeValuePairFilterVisitor<Boolean> {
    @Override
    public <T extends Comparable<T>> Boolean satisfy(TimeValuePair value, Filter<T> filter) {
        return filter.accept(value, this);
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(TimeValuePair value, Eq<T> eq) {
        Object v = (eq instanceof TimeEq) ? value.getTimestamp() : value.getValue().getValue();
        return eq.getValue().equals(v);
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(TimeValuePair value, NotEq<T> notEq) {
        Object v = (notEq instanceof TimeNotEq) ? value.getTimestamp() : value.getValue().getValue();
        return !notEq.getValue().equals(v);
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(TimeValuePair value, LtEq<T> ltEq) {
        Object v = (ltEq instanceof TimeLtEq) ? value.getTimestamp() : value.getValue().getValue();
        if (ltEq.getValue().compareTo((T) v) >= 0) {
            return true;
        }
        return false;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(TimeValuePair value, GtEq<T> gtEq) {
        Object v = (gtEq instanceof TimeGtEq) ? value.getTimestamp() : value.getValue().getValue();
        if (gtEq.getValue().compareTo((T) v) <= 0) {
            return true;
        }
        return false;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(TimeValuePair value, Gt<T> gt) {
        Object v = (gt instanceof TimeGt) ? value.getTimestamp() : value.getValue().getValue();
        if (gt.getValue().compareTo((T) v) < 0) {
            return true;
        }
        return false;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(TimeValuePair value, Lt<T> lt) {
        Object v = (lt instanceof TimeLt) ? value.getTimestamp() : value.getValue().getValue();
        if (lt.getValue().compareTo((T) v) > 0) {
            return true;
        }
        return false;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(TimeValuePair value, Not<T> not) {
        return !satisfy(value, not.getFilterExpression());
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(TimeValuePair value, And<T> and) {
        return satisfy(value, and.getLeft()) && satisfy(value, and.getRight());
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(TimeValuePair value, Or<T> or) {
        return satisfy(value, or.getLeft()) || satisfy(value, or.getRight());
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(TimeValuePair value, NoRestriction<T> noFilter) {
        return true;
    }


}
