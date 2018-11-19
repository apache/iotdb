package cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.impl;


import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.UnaryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.factory.FilterType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.operator.*;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.visitor.AbstractFilterVisitor;

/**
 * Created by zhangjinrui on 2017/12/24.
 */
public class DigestFilterVisitor implements AbstractFilterVisitor<Boolean> {

    private ThreadLocal<DigestForFilter> timestampDigest;
    private ThreadLocal<DigestForFilter> valueDigest;
    private ThreadLocal<Comparable<?>> minValue;
    private ThreadLocal<Comparable<?>> maxValue;

    public DigestFilterVisitor() {
        this.timestampDigest = new ThreadLocal<>();
        this.valueDigest = new ThreadLocal<>();
        this.minValue = new ThreadLocal<>();
        this.maxValue = new ThreadLocal<>();
    }

    public Boolean satisfy(DigestForFilter timestampDigest, DigestForFilter valueDigest, Filter<?> filter) {
        this.timestampDigest.set(timestampDigest);
        this.valueDigest.set(valueDigest);
        return filter.accept(this);
    }

    private void prepareMaxAndMinValue(UnaryFilter<?> unaryFilter) {
        if (unaryFilter.getFilterType() == FilterType.TIME_FILTER) {
            this.minValue.set(timestampDigest.get().getMinValue());
            this.maxValue.set(timestampDigest.get().getMaxValue());
        } else {
            this.minValue.set(valueDigest.get().getMinValue());
            this.maxValue.set(valueDigest.get().getMaxValue());
        }
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
        prepareMaxAndMinValue(eq);
        return eq.getValue().compareTo((T) minValue.get()) >= 0
                && eq.getValue().compareTo((T) maxValue.get()) <= 0;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
        prepareMaxAndMinValue(notEq);
        return notEq.getValue().compareTo((T) minValue.get()) == 0
                && notEq.getValue().compareTo((T) maxValue.get()) == 0;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
        prepareMaxAndMinValue(ltEq);
        return ltEq.getValue().compareTo((T) minValue.get()) >= 0;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
        prepareMaxAndMinValue(gtEq);
        return gtEq.getValue().compareTo((T) maxValue.get()) <= 0;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Gt<T> gt) {
        prepareMaxAndMinValue(gt);
        return gt.getValue().compareTo((T) maxValue.get()) < 0;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Lt<T> lt) {
        prepareMaxAndMinValue(lt);
        return lt.getValue().compareTo((T) minValue.get()) > 0;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Not<T> not) {
        return !satisfy(timestampDigest.get(), valueDigest.get(), not.getFilterExpression());
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(And<T> and) {
        return satisfy(timestampDigest.get(), valueDigest.get(), and.getLeft())
                && satisfy(timestampDigest.get(), valueDigest.get(), and.getRight());
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Or<T> or) {
        return satisfy(timestampDigest.get(), valueDigest.get(), or.getLeft())
                || satisfy(timestampDigest.get(), valueDigest.get(), or.getRight());
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(NoRestriction<T> noFilter) {
        return true;
    }
}
