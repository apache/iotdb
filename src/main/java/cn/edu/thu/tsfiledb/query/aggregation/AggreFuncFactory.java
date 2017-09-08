package cn.edu.thu.tsfiledb.query.aggregation;

import cn.edu.thu.tsfiledb.query.aggregation.impl.CountAggrFunc;
import cn.edu.thu.tsfiledb.query.aggregation.impl.MaxTimeAggrFunc;
import cn.edu.thu.tsfiledb.query.aggregation.impl.MaxValueAggrFunc;
import cn.edu.thu.tsfiledb.query.aggregation.impl.MinTimeAggrFunc;
import cn.edu.thu.tsfiledb.query.aggregation.impl.MinValueAggrFunc;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

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
            default:
                throw new ProcessorException("AggregateFunction not support. Name:" + aggrFuncName);
        }
    }
}
