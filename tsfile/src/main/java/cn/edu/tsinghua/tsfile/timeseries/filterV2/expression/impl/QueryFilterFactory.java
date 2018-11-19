package cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.BinaryQueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilterType;

/**
 * Created by zhangjinrui on 2017/12/18.
 */
public abstract class QueryFilterFactory implements BinaryQueryFilter {

    protected static class And extends QueryFilterFactory {
        public QueryFilter left;
        public QueryFilter right;

        public And(QueryFilter left, QueryFilter right){
            this.left = left;
            this.right = right;
        }

        @Override
        public QueryFilter getLeft() {
            return left;
        }

        @Override
        public QueryFilter getRight() {
            return right;
        }

        @Override
        public QueryFilterType getType() {
            return QueryFilterType.AND;
        }

        public String toString() {
            return "[" + left + " && " + right + "]";
        }
    }

    protected static class Or extends QueryFilterFactory {
        public QueryFilter left;
        public QueryFilter right;
        public Or(QueryFilter left, QueryFilter right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public QueryFilter getLeft() {
            return left;
        }
        @Override
        public QueryFilter getRight() {
            return right;
        }

        public QueryFilterType getType() {
            return QueryFilterType.OR;
        }

        public String toString() {
            return "[" + left + " || " + right + "]";
        }
    }

    public static BinaryQueryFilter and(QueryFilter left, QueryFilter right){
        return new And(left, right);
    }

    public static BinaryQueryFilter or(QueryFilter left, QueryFilter right) {
        return new Or(left, right);
    }
}
