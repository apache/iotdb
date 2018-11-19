package cn.edu.tsinghua.tsfile.timeseries.filterV2.expression;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;


/**
 * @author Jinrui Zhang
 */
public interface UnaryQueryFilter extends QueryFilter{
    Filter<?> getFilter();
}
