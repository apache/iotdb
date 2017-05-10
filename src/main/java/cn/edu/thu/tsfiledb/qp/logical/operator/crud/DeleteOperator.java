package cn.edu.thu.tsfiledb.qp.logical.operator.crud;

import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.LESSTHAN;
import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.LESSTHANOREQUALTO;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.ParseTimeException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QpSelectFromException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
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
    public PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor conf)
            throws QueryProcessorException {
        DeletePlan delPlan = new DeletePlan();
        // thansform where condition to Filter class
        // for update, delete and insert, all where clause has just condition about time
        parseDeleteTimeFilter(delPlan);
        // parse path
        List<Path> paths = getSelSeriesPaths(conf);
        if (paths.size() != 1) {
            throw new QpSelectFromException(
                    "for delete command, cannot specified more than one path:" + paths);
        }
        delPlan.setPath(paths.get(0));
        return delPlan;
    }

    /**
     * for delete command, time should have start and end time range.
     * 
     * @param delPlan
     */
    private void parseDeleteTimeFilter(DeletePlan delPlan) throws ParseTimeException {
        if (!(filterOperator.isLeaf)) {
            // LOG.error("for delete command, where clause must be like : time < XXXX");
            throw new ParseTimeException(
                    "for delete command, where clause must be like : time < XXX");
        }

        if (filterOperator.getTokenIntType() != LESSTHAN
                && filterOperator.getTokenIntType() != LESSTHANOREQUALTO) {
            // LOG.error(
            // "for delete command, time filter must be less than or less than or equal to, this:{}",
            // filterOperator.getTokenIntType());
            // return false;
            throw new ParseTimeException(
                    "for delete command, time filter must be less than or less than or equal to, this:"
                            + filterOperator.getTokenIntType());
        }
        long delTime = Long.valueOf(((BasicFunctionOperator) filterOperator).getSeriesValue());

        if (delTime < 0) {
            // LOG.error("delete Time:{}, time filter error", delTime);
            // return false;
            throw new ParseTimeException("delete Time:" + delTime + ", time filter error");
        }
        delPlan.setDeleteTime(delTime);
    }


}
