package cn.edu.thu.tsfiledb.query.aggregation;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfiledb.query.aggregation.impl.CountAggrFunc;
import cn.edu.thu.tsfiledb.query.aggregation.impl.MaxTimeAggrFunc;
import cn.edu.thu.tsfiledb.query.aggregation.impl.MaxValueAggrFunc;
import cn.edu.thu.tsfiledb.query.aggregation.impl.MinTimeAggrFunc;
import cn.edu.thu.tsfiledb.query.aggregation.impl.MinValueAggrFunc;

public class AggreFuncFactory {
    public static AggregateFunction getAggrFuncByName(String aggrFuncName, TSDataType dataType) throws ProcessorException {
        if (aggrFuncName == null) {
            throw new ProcessorException("AggregateFunction Name must not be null");
        }
        else if (aggrFuncName.toLowerCase().equals("min_timestamp")) {
            return new MinTimeAggrFunc();
        }
        else if (aggrFuncName.toLowerCase().equals("max_timestamp")) {
            return new MaxTimeAggrFunc();
        }
        else if (aggrFuncName.toLowerCase().equals("max_value")) {
            return new MaxValueAggrFunc(dataType);
        }
        else if (aggrFuncName.toLowerCase().equals("min_value")) {
            return new MinValueAggrFunc(dataType);
        }
        else if (aggrFuncName.toLowerCase().equals("count")) {
            return new CountAggrFunc();
        }
        else
            throw new ProcessorException("AggregateFunction not support. Name:" + aggrFuncName);
    }
}
