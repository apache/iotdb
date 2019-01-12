package org.apache.iotdb.tsfile.read.expression.impl;

import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IUnaryExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;


public class GlobalTimeExpression implements IUnaryExpression {
    private Filter filter;

    public GlobalTimeExpression(Filter filter) {
        this.filter = filter;
    }

    @Override
    public Filter getFilter() {
        return filter;
    }

    @Override
    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.GLOBAL_TIME;
    }

    @Override
    public String toString() {
        return "[" + this.filter.toString() + "]";
    }
}
