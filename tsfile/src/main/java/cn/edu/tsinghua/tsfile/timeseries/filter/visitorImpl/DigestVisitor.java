package cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl;

import cn.edu.tsinghua.tsfile.common.exception.FilterDataTypeException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To judge whether a series, page could be skipped when reading process
 * Implemented using visitor pattern.
 *
 * @author CGF
 */
public class DigestVisitor implements FilterVisitor<Boolean> {
    private static final Logger LOG = LoggerFactory.getLogger(DigestVisitor.class);

    private DigestForFilter digest;

    public Boolean satisfy(DigestForFilter digest, SingleSeriesFilterExpression expression) {
        if (expression == null)
            return true;

        this.digest = digest;
        return expression.accept(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
        if (!digest.getType().equals(eq.getFilterSeries().getSeriesDataType())) {
            LOG.error("Generic Not Consistent! {} does't match {}", digest.getTypeClass(),
                    eq.getFilterSeries().getSeriesDataType());
            throw new FilterDataTypeException("Generic Not Consistent! " + digest.getTypeClass() + " does't match " +
                    eq.getFilterSeries().getSeriesDataType());
        }
        try {
            return eq.getValue().compareTo((T) digest.getMinValue()) >= 0
                    && eq.getValue().compareTo((T) digest.getMaxValue()) <= 0;
        } catch (NullPointerException e) {
            LOG.error("The value of SingleSensorFilter {} is null", eq);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
        if (!digest.getType().equals(notEq.getFilterSeries().getSeriesDataType())) {
            LOG.error("Generic Not Consistent! {} does't match {}", digest.getTypeClass(),
                    notEq.getFilterSeries().getSeriesDataType());
            throw new FilterDataTypeException("Generic Not Consistent! " + digest.getTypeClass() + " does't match " +
                    notEq.getFilterSeries().getSeriesDataType());
        }

        try {
            return notEq.getValue().compareTo((T) digest.getMinValue()) != 0
                    && notEq.getValue().compareTo((T) digest.getMaxValue()) != 0;
        } catch (NullPointerException e) {
            LOG.error("The value of SingleSensorFilter {} is null", notEq);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
        if (!digest.getType().equals(ltEq.getFilterSeries().getSeriesDataType())) {
            LOG.error("Generic Not Consistent! {} does't match {}", digest.getTypeClass(),
                    ltEq.getFilterSeries().getSeriesDataType());
            throw new FilterDataTypeException("Generic Not Consistent! " + digest.getTypeClass() + " does't match " +
                    ltEq.getFilterSeries().getSeriesDataType());
        }

        try {
            if (ltEq.getIfEq()) {
                return ltEq.getValue().compareTo((T) digest.getMinValue()) >= 0;
            } else {
                return ltEq.getValue().compareTo((T) digest.getMinValue()) > 0;
            }
        } catch (NullPointerException e) {
            LOG.error("The value of SingleSensorFilter {} is null", ltEq);
            return false;
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
        if (!digest.getType().equals(gtEq.getFilterSeries().getSeriesDataType())) {
            LOG.error("Generic Not Consistent! {} does't match {}", digest.getTypeClass(),
                    gtEq.getFilterSeries().getSeriesDataType());
            throw new FilterDataTypeException("Generic Not Consistent! " + digest.getTypeClass() + " does't match " +
                    gtEq.getFilterSeries().getSeriesDataType());
        }

        try {
            if (gtEq.getIfEq()) {
                return gtEq.getValue().compareTo((T) digest.getMaxValue()) <= 0;
            } else {
                return gtEq.getValue().compareTo((T) digest.getMaxValue()) < 0;
            }
        } catch (NullPointerException e) {
            LOG.error("The value of SingleSensorFilter {} is null", gtEq);
            return false;
        }
    }

    @Override
    public Boolean visit(Not not) {
        return !satisfy(digest, not.getFilterExpression());
    }

    @Override
    public Boolean visit(And and) {
        return satisfy(digest, and.getLeft()) && satisfy(digest, and.getRight());
    }

    @Override
    public Boolean visit(Or or) {
        return satisfy(digest, or.getLeft()) || satisfy(digest, or.getRight());
    }

    @Override
    public Boolean visit(NoFilter noFilter) {
        return true;
    }

}
