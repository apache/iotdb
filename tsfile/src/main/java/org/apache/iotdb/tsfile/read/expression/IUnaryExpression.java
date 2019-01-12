package org.apache.iotdb.tsfile.read.expression;

import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public interface IUnaryExpression extends IExpression {

    Filter getFilter();

    void setFilter(Filter filter);
}

