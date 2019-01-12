package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.logical.Operator;

import java.util.List;

public class QueryPlan extends PhysicalPlan{

    private List<Path> paths = null;
    private IExpression expression = null;

    public QueryPlan() {
        super(true);
        setOperatorType(Operator.OperatorType.QUERY);
    }

    public QueryPlan(boolean isQuery, Operator.OperatorType operatorType) {
        super(isQuery, operatorType);
    }

    /**
     * check if all paths exist
     */
    public void checkPaths(QueryProcessExecutor executor) throws QueryProcessorException {
        for (Path path : paths) {
            if (!executor.judgePathExists(path)) {
                throw new QueryProcessorException("Path doesn't exist: " + path);
            }
        }
    }

    public IExpression getExpression() {
        return expression;
    }

    public void setExpression(IExpression expression) {
        this.expression = expression;
    }

    @Override
    public List<Path> getPaths() {
        return paths;
    }

    public void setPaths(List<Path> paths) {
        this.paths = paths;
    }
}
