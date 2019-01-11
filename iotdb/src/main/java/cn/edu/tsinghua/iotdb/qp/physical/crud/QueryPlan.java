package cn.edu.tsinghua.iotdb.qp.physical.crud;

import cn.edu.tsinghua.iotdb.exception.qp.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.executor.QueryProcessExecutor;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;

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
