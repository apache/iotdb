package cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw;

import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.LESSTHAN;
import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.LESSTHANOREQUALTO;

import java.util.List;

import cn.edu.thu.tsfiledb.qp.logical.operator.clause.filter.BasicFunctionOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.ParseTimeException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QpSelectFromException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.physical.plan.DeletePlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;

/**
 * this class extends {@code RootOperator} and process delete statement
 * 
 * @author kangrong
 *
 */
public class DeleteOperator extends SFWOperator {

    private static Logger LOG = LoggerFactory.getLogger(DeleteOperator.class);

    public DeleteOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.DELETE;
    }

    @Override
    public PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor executor)
            throws QueryProcessorException {
        DeletePlan delPlan = new DeletePlan();
        // transform where condition to Filter class
        // for update, delete and insert, all where clause has just condition about time
        parseDeleteTimeFilter(delPlan);
        // parse path
        List<Path> paths = getSelSeriesPaths(executor);
        if (paths.size() != 1) {
            throw new QpSelectFromException(
                    "for delete command, cannot specified more than one path:" + paths);
        }
        delPlan.setPath(paths.get(0));
        return delPlan;
    }

    /**
     * for delete command, time should only have an end time.
     * 
     * @param delPlan delete physical plan
     */
    private void parseDeleteTimeFilter(DeletePlan delPlan) throws ParseTimeException {
        if (!(filterOperator.isLeaf())) {
            throw new ParseTimeException(
                    "for delete command, where clause must be like : time < XXX or time <= XXX");
        }

        if (filterOperator.getTokenIntType() != LESSTHAN
                && filterOperator.getTokenIntType() != LESSTHANOREQUALTO) {
            throw new ParseTimeException(
                    "for delete command, time filter must be less than or less than or equal to, this:"
                            + filterOperator.getTokenIntType());
        }
        long time = Long.valueOf(((BasicFunctionOperator) filterOperator).getValue());

        if (time < 0) {
            throw new ParseTimeException("delete Time:" + time + ", time must >= 0");
        }
        delPlan.setDeleteTime(time);
    }


}
