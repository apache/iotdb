package cn.edu.tsinghua.tsfile.read.expression;

import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;

public interface IUnaryExpression extends IExpression {

    Filter getFilter();

    void setFilter(Filter filter);
}

