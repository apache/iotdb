package cn.edu.thu.tsfiledb.qp.physical;

import java.util.Iterator;
import java.util.List;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;

/**
 * This class is a abstract class for all type of PhysicalPlan.
 * 
 * @author kangrong
 *
 */
public abstract class PhysicalPlan {
    private boolean isQuery;
    private OperatorType operatorType;

    protected PhysicalPlan(boolean isQuery, OperatorType operatorType) {
        this.isQuery = isQuery;
        this.operatorType = operatorType;
    }

    public Iterator<QueryDataSet> processQuery(QueryProcessExecutor executor) throws QueryProcessorException {
        throw new UnsupportedOperationException();
    }

    public boolean processNonQuery(QueryProcessExecutor config) throws ProcessorException {
        throw new UnsupportedOperationException();
    }

    public String printQueryPlan() {
        return "abstract plan";
    }

    public abstract List<Path> getPaths();

    public boolean isQuery(){
        return isQuery;
    }

    public OperatorType getOperatorType() {
        return operatorType;
    }

}
