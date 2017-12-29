package cn.edu.tsinghua.iotdb.query.aggregation;

import cn.edu.tsinghua.iotdb.query.aggregation.impl.*;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * Easy factory pattern to build AggregateFunction.
 *
 */
public class AggreFuncFactory {

    public static AggregateFunction getAggrFuncByName(String aggrFuncName, TSDataType dataType) throws ProcessorException {
        if (aggrFuncName == null) {
            throw new ProcessorException("AggregateFunction Name must not be null");
        }

        switch (aggrFuncName.toLowerCase()) {
            case AggregationConstant.MIN_TIME:
                return new MinTimeAggrFunc();
            case AggregationConstant.MAX_TIME:
                return new MaxTimeAggrFunc();
            case AggregationConstant.MIN_VALUE:
                return new MinValueAggrFunc(dataType);
            case AggregationConstant.MAX_VALUE:
                return new MaxValueAggrFunc(dataType);
            case AggregationConstant.COUNT:
                return new CountAggrFunc();
            case AggregationConstant.MEAN:
                return new MeanAggrFunc();
            case AggregationConstant.FIRST:
                return new FirstAggrFunc(dataType);
            case AggregationConstant.SUM:
                return new SumAggrFunc();
            default:
                throw new ProcessorException("aggregate does not support " + aggrFuncName + " function.");
        }
    }
}
