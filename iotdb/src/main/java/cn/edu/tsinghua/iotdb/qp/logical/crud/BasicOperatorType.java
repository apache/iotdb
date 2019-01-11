package cn.edu.tsinghua.iotdb.qp.logical.crud;

import cn.edu.tsinghua.iotdb.qp.constant.SQLConstant;
import cn.edu.tsinghua.iotdb.exception.qp.LogicalOperatorException;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.IUnaryExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.GlobalTimeExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.read.filter.ValueFilter;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;

/**
 * all basic operator in filter
 */
public enum BasicOperatorType {
    EQ {
        @Override
        public <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value) {
            if(path.equals("time")) {
                return new GlobalTimeExpression(TimeFilter.eq((Long) value));
            } else {
                return new SingleSeriesExpression(path, ValueFilter.eq(value));
            }
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
        public <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value) {
            if(path.equals("time")) {
                return new GlobalTimeExpression(TimeFilter.ltEq((Long) value));
            } else {
                return new SingleSeriesExpression(path, ValueFilter.ltEq(value));
            }
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
        public <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value) {
            if(path.equals("time")) {
                return new GlobalTimeExpression(TimeFilter.lt((Long) value));
            } else {
                return new SingleSeriesExpression(path, ValueFilter.lt(value));
            }
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
        public <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value) {
            if(path.equals("time")) {
                return new GlobalTimeExpression(TimeFilter.gtEq((Long) value));
            } else {
                return new SingleSeriesExpression(path, ValueFilter.gtEq(value));
            }
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
        public <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value) {
            if(path.equals("time")) {
                return new GlobalTimeExpression(TimeFilter.gt((Long) value));
            } else {
                return new SingleSeriesExpression(path, ValueFilter.gt(value));
            }
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
        public <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value) {
            if(path.equals("time")) {
                return new GlobalTimeExpression(TimeFilter.notEq((Long) value));
            } else {
                return new SingleSeriesExpression(path, ValueFilter.notEq(value));
            }
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

    public abstract <T extends Comparable<T>> IUnaryExpression getUnaryExpression(Path path, T value);

    public abstract <T extends Comparable<T>> Filter getValueFilter(T tsPrimitiveType);

    public abstract Filter getTimeFilter(long value);
}
