package cn.edu.thu.tsfiledb.qp.logical.operator.root.sfw;

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
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.clause.filter.BasicFunctionOperator;
import cn.edu.thu.tsfiledb.qp.logical.operator.clause.filter.FilterOperator;
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

    public void setValue(String value) throws BasicOperatorException {
        this.value = value;
    }

    @Override
    public PhysicalPlan transformToPhysicalPlan(QueryProcessExecutor conf)
            throws QueryProcessorException {
        UpdatePlan upPlan = new UpdatePlan();
        // transform where condition to time bounds in physical plan
        // for update, delete and insert, all where clause has just condition about time
        parseUpdateTimeFilter(upPlan);
        // parse value
        upPlan.setValue(value);
        // parse path
        List<Path> paths = getSelSeriesPaths(conf);
        if (paths.size() > 1) {
            throw new QpSelectFromException("update command, must have and only have one path:"
                    + paths);
        }
        upPlan.setPath(paths.get(0));
        return upPlan;
    }

    /**
     * for update command, time should have start and end time range.
     * 
     * @param upPlan
     */
    private void parseUpdateTimeFilter(UpdatePlan upPlan) throws ParseTimeException {
        if (!filterOperator.isSingle() || !filterOperator.getSinglePath().equals(RESERVED_TIME)) {
            throw new ParseTimeException(
                    "for update command, it has non-time condition in where clause");
        }
        if (filterOperator.getChildren().size() != 2)
            throw new ParseTimeException(
                    "for update command, time must have left and right boundaries");

        long startTime = -1;
        long endTime = -1;

        for(FilterOperator operator: filterOperator.getChildren()) {
            if(!operator.isLeaf())
                throw new ParseTimeException("illegal time condition:" + filterOperator.showTree());
            switch (operator.getTokenIntType()) {
                case LESSTHAN: endTime = Long.valueOf(((BasicFunctionOperator) operator).getValue()) - 1; break;
                case LESSTHANOREQUALTO: endTime = Long.valueOf(((BasicFunctionOperator) operator).getValue()); break;
                case GREATERTHAN:
                    startTime = Long.valueOf(((BasicFunctionOperator) operator).getValue());
                    if(startTime < Long.MAX_VALUE)
                        startTime++; break;
                case GREATERTHANOREQUALTO: startTime = Long.valueOf(((BasicFunctionOperator) operator).getValue()); break;
                default: throw new ParseTimeException("time filter must be >,<,>=,<=");
            }
        }
        
        if (startTime < 0 || endTime < 0) {
            throw new ParseTimeException("startTime:" + startTime + ",endTime:" + endTime
                    + ", one of them is illegal");
        }
        if (startTime > endTime) {
            throw new ParseTimeException("startTime:" + startTime + ",endTime:" + endTime
                    + ", start time cannot be greater than end time");
        }
        upPlan.setStartTime(startTime);
        upPlan.setEndTime(endTime);
    }
}
