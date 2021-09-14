package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.qp.constant.SQLConstant;
import org.apache.iotdb.infludb.query.expression.Expression;

import javax.swing.plaf.nimbus.AbstractRegionPainter;
import java.util.List;

public class FunctionFactory {
    public static Function generateFunction(String name, List<Expression> expressionList) {
        switch (name) {
            case SQLConstant.MAX:
                return new MaxFunction(expressionList);
            case SQLConstant.MIN:
                return new MinFunction(expressionList);
            case SQLConstant.MEAN:
                return new MeanFunction(expressionList);
            case SQLConstant.LAST:
                return new LastFunction(expressionList);
            case SQLConstant.FIRST:
                return new FirstFunction(expressionList);
            case SQLConstant.COUNT:
                return new CountFunction(expressionList);
            case SQLConstant.MEDIAN:
                return new MedianFunction(expressionList);
            case SQLConstant.MODE:
                return new ModeFunction(expressionList);
            case SQLConstant.SPREAD:
                return new SpreadFunction(expressionList);
            case SQLConstant.STDDEV:
                return new StddevFunction(expressionList);
            case SQLConstant.SUM:
                return new SumFunction(expressionList);
            default:
                throw new IllegalArgumentException("not support aggregation name:" + name);
        }
    }
}
