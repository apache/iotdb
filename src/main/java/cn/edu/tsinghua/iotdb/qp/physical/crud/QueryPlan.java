package cn.edu.tsinghua.iotdb.qp.physical.crud;

import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;
import cn.edu.tsinghua.iotdb.qp.executor.QueryProcessExecutor;
import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.List;

public class QueryPlan extends PhysicalPlan{

    private List<Path> paths = null;
    private QueryFilter queryFilter = null;

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
            if (!executor.judgePathExists(path))
                throw new QueryProcessorException("Path doesn't exist: " + path);
        }
    }

    public QueryFilter getQueryFilter() {
        return queryFilter;
    }

    public void setQueryFilter(QueryFilter queryFilter) {
        this.queryFilter = queryFilter;
    }

    @Override
    public List<Path> getPaths() {
        return paths;
    }

    public void setPaths(List<Path> paths) {
        this.paths = paths;
    }
}
