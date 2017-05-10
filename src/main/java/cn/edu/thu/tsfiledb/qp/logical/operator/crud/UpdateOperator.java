package cn.edu.thu.tsfiledb.qp.logical.operator.crud;

import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.GREATERTHAN;
import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.GREATERTHANOREQUALTO;
import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.LESSTHAN;
import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.LESSTHANOREQUALTO;
import static cn.edu.thu.tsfiledb.qp.constant.SQLConstant.RESERVED_TIME;

import java.util.List;

import cn.edu.thu.tsfiledb.qp.exception.QueryProcessorException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.BasicOperatorException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.ParseTimeException;
import cn.edu.thu.tsfiledb.qp.exception.logical.operator.QpSelectFromException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.physical.plan.PhysicalPlan;
import cn.edu.thu.tsfiledb.qp.physical.plan.UpdatePlan;

/**
 * this class extends {@code RootOperator} and process update statement
 * 
 * @author kangrong
 *
 */
public final class UpdateOperator extends SFWOperator {
    private String value;

    public UpdateOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = OperatorType.UPDATE;
    }

    public void setUpdateValue(String value) throws BasicOperatorException {
        this.value = value;
    }

    @Override
    public PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor conf)
            throws QueryProcessorException {
        UpdatePlan upPlan = new UpdatePlan();
        // thansform where condition to Filter class
        // for update, delete and insert, all where clause has just condition about time
        parseUpdateTimeFilter(upPlan);
        // parse value
        upPlan.setValue(value);
        // parse path
        List<Path> paths = getSelSeriesPaths(conf);
        if (paths.size() != 1) {
            throw new QpSelectFromException("update command, must have and only have one path:"
                    + paths);
            // LOG.error("for update command, cannot specified more than one path:{}", paths);
        }
        upPlan.setPath(paths.get(0));
        return upPlan;
    }

    /**
     * for udpate command, time should have start and end time range.
     * 
     * @param upPlan
     */
    private void parseUpdateTimeFilter(UpdatePlan upPlan) throws ParseTimeException {
        if (!filterOperator.isSingle || !filterOperator.getSinglePath().equals(RESERVED_TIME)) {
            throw new ParseTimeException(
                    "for update command, it has non-time condition in where clause");
        }
        if (filterOperator.childOperators.size() != 2)
            throw new ParseTimeException(
                    "for update command, time must have left and right boundaries");
        FilterOperator leftOp = filterOperator.childOperators.get(0);
        FilterOperator rightOp = filterOperator.childOperators.get(1);
        if (!leftOp.isLeaf || !rightOp.isLeaf)
            throw new ParseTimeException("illegal time condition:" + filterOperator.showTree());
        long startTime = -1;
        long endTime = -1;
        // left time border

        if (leftOp.getTokenIntType() == LESSTHAN || leftOp.getTokenIntType() == LESSTHANOREQUALTO) {
            endTime = Long.valueOf(((BasicFunctionOperator) leftOp).getSeriesValue());
        } else if (leftOp.getTokenIntType() == GREATERTHAN
                || leftOp.getTokenIntType() == GREATERTHANOREQUALTO) {
            startTime = Long.valueOf(((BasicFunctionOperator) leftOp).getSeriesValue());
        }
        if(leftOp.getTokenIntType() == LESSTHAN){
        	endTime --;
        }
        if(leftOp.getTokenIntType() == GREATERTHAN && startTime < Long.MAX_VALUE){
        	startTime ++;
        }
        // right time border
        if (rightOp.getTokenIntType() == LESSTHAN || rightOp.getTokenIntType() == LESSTHANOREQUALTO) {
            endTime = Long.valueOf(((BasicFunctionOperator) rightOp).getSeriesValue());
        } else if (rightOp.getTokenIntType() == GREATERTHAN
                || rightOp.getTokenIntType() == GREATERTHANOREQUALTO) {
            startTime = Long.valueOf(((BasicFunctionOperator) rightOp).getSeriesValue());
        }
        if(rightOp.getTokenIntType() == LESSTHAN){
        	endTime --;
        }
        if(rightOp.getTokenIntType() == GREATERTHAN && startTime < Long.MAX_VALUE){
        	startTime ++;
        }
        
        if (startTime < 0 || endTime < 0) {
            // LOG.error("startTime:{},endTime:{}, one of them is illegal", startTime, endTime);
            throw new ParseTimeException("startTime:" + startTime + ",endTime:" + endTime
                    + ", one of them is illegal");
        }
        if (startTime > endTime) {
            // LOG.error("startTime:{},endTime:{}, start time cannot be greater than end time",
            // startTime, endTime);
            throw new ParseTimeException("startTime:" + startTime + ",endTime:" + endTime
                    + ", start time cannot be greater than end time");
        }
        upPlan.setStartTime(startTime);
        upPlan.setEndTime(endTime);
    }
}
