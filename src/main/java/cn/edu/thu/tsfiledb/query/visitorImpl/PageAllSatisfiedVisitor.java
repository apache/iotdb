package cn.edu.thu.tsfiledb.query.visitorImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.And;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.Eq;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.LtEq;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.Not;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.NotEq;
import cn.edu.thu.tsfile.timeseries.filter.definition.operators.Or;
import cn.edu.thu.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.thu.tsfile.timeseries.filter.visitorImpl.FilterVisitor;


public class PageAllSatisfiedVisitor implements FilterVisitor<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PageAllSatisfiedVisitor.class);
    public DigestForFilter digest;

    public Boolean satisfy(DigestForFilter digest, SingleSeriesFilterExpression expression) {
        this.digest = digest;
        return expression.accept(this);
    }

    private boolean checkType(SingleSeriesFilterExpression expression) {
        if (!digest.getType().equals(expression.getFilterSeries().getSeriesDataType())) {
            LOGGER.error(digest.getTypeClass() + " - " + expression.getFilterSeries().getSeriesDataType());
            LOGGER.error("Generic Not Consistent!");
            return false;
        }
        return true;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(Eq<T> eq) {
        if (!checkType(eq)) {
            return false;
        }
        return eq.getValue().compareTo((T) digest.getMinValue()) == 0
                && eq.getValue().compareTo((T) digest.getMaxValue()) == 0;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(NotEq<T> notEq) {
        if (!checkType(notEq)) {
            return false;
        }
        return notEq.getValue().compareTo((T) digest.getMinValue()) < 0 ||
                notEq.getValue().compareTo((T) digest.getMaxValue()) > 0;
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(LtEq<T> ltEq) {
        if (!checkType(ltEq)) {
            return false;
        }
        if (ltEq.getIfEq()) {
            return ltEq.getValue().compareTo((T) digest.getMaxValue()) >= 0;
        } else {
            return ltEq.getValue().compareTo((T) digest.getMaxValue()) > 0;
        }
    }

    @Override
    public <T extends Comparable<T>> Boolean visit(GtEq<T> gtEq) {
        if (!checkType(gtEq)) {
            return false;
        }
        if (gtEq.getIfEq()) {
            return gtEq.getValue().compareTo((T) digest.getMinValue()) <= 0;
        } else {
            return gtEq.getValue().compareTo((T) digest.getMinValue()) < 0;
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

}
