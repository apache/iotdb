package cn.edu.tsinghua.iotdb.qp.logical.crud;

import cn.edu.tsinghua.iotdb.qp.constant.SQLConstant;
import cn.edu.tsinghua.iotdb.qp.exception.LogicalOperatorException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

/**
 * all basic operator in filter
 * 
 * @author kangrong
 *
 */
public enum BasicOperatorType {
    EQ {
        @Override
        public <T extends Comparable<T>, C extends FilterSeries<T>> SingleSeriesFilterExpression getValueFilter(
                C column, T value) {
            return FilterFactory.eq(column, value);
        }

        @Override
        public <T extends Comparable<T>> Filter getValueFilter(T value) {
            return ValueFilter.eq(value);
        }

        @Override
        public Filter getTimeFilter(long value) {
            return TimeFilter.eq(value);
        }
    },
    LTEQ {
        @Override
        public <T extends Comparable<T>, C extends FilterSeries<T>> SingleSeriesFilterExpression getValueFilter(
                C column, T value) {
            return FilterFactory.ltEq(column, value, true);
        }

        @Override
        public <T extends Comparable<T>> Filter getValueFilter(T value) {
            return ValueFilter.ltEq(value);
        }

        @Override
        public Filter getTimeFilter(long value) {
            return TimeFilter.ltEq(value);
        }
    },
    LT {
        @Override
        public <T extends Comparable<T>, C extends FilterSeries<T>> SingleSeriesFilterExpression getValueFilter(
                C column, T value) {
            return FilterFactory.ltEq(column, value, false);
        }

        @Override
        public <T extends Comparable<T>> Filter getValueFilter(T value) {
            return ValueFilter.lt(value);
        }

        @Override
        public Filter getTimeFilter(long value) {
            return TimeFilter.lt(value);
        }
    },
    GTEQ {
        @Override
        public <T extends Comparable<T>, C extends FilterSeries<T>> SingleSeriesFilterExpression getValueFilter(
                C column, T value) {
            return FilterFactory.gtEq(column, value, true);
        }

        @Override
        public <T extends Comparable<T>> Filter getValueFilter(T value) {
            return ValueFilter.gtEq(value);
        }

        @Override
        public Filter getTimeFilter(long value) {
            return TimeFilter.gtEq(value);
        }
    },
    GT {
        @Override
        public <T extends Comparable<T>, C extends FilterSeries<T>> SingleSeriesFilterExpression getValueFilter(
                C column, T value) {
            return FilterFactory.gtEq(column, value, false);
        }

        @Override
        public <T extends Comparable<T>> Filter getValueFilter(T value) {
            return ValueFilter.gt(value);
        }

        @Override
        public Filter getTimeFilter(long value) {
            return TimeFilter.gt(value);
        }
    },
    NOTEQUAL {
        @Override
        public <T extends Comparable<T>, C extends FilterSeries<T>> SingleSeriesFilterExpression getValueFilter(
                C column, T value) {
            return FilterFactory.noteq(column, value);
        }

        @Override
        public <T extends Comparable<T>> Filter getValueFilter(T value) {
            return ValueFilter.notEq(value);
        }

        @Override
        public Filter getTimeFilter(long value) {
            return TimeFilter.notEq(value);
        }
    };

    public static BasicOperatorType getBasicOpBySymbol(int tokenIntType)
            throws LogicalOperatorException {
        switch (tokenIntType) {
            case SQLConstant.EQUAL:
                return EQ;
            case SQLConstant.LESSTHANOREQUALTO:
                return LTEQ;
            case SQLConstant.LESSTHAN:
                return LT;
            case SQLConstant.GREATERTHANOREQUALTO:
                return GTEQ;
            case SQLConstant.GREATERTHAN:
                return GT;
            case SQLConstant.NOTEQUAL:
                return NOTEQUAL;
            default:
                throw new LogicalOperatorException("unsupported type:{}"
                        + SQLConstant.tokenNames.get(tokenIntType));
        }
    }

    public abstract <T extends Comparable<T>, C extends FilterSeries<T>> SingleSeriesFilterExpression getValueFilter(
            C column, T value);

    public abstract <T extends Comparable<T>> Filter getValueFilter(T tsPrimitiveType);

    public abstract Filter getTimeFilter(long value);
}
