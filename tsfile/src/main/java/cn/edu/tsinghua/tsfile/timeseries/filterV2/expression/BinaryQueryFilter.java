package cn.edu.tsinghua.tsfile.timeseries.filterV2.expression;

/**
 * @author Jinrui Zhang
 */
public interface BinaryQueryFilter extends QueryFilter{
    QueryFilter getLeft();

    QueryFilter getRight();


}
