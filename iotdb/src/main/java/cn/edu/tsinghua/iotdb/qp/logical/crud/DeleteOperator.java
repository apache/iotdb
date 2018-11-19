package cn.edu.tsinghua.iotdb.qp.logical.crud;

import cn.edu.tsinghua.iotdb.qp.logical.Operator;

/**
 * this class extends {@code RootOperator} and process delete statement
 * 
 * @author kangrong
 *
 */
public class DeleteOperator extends SFWOperator {

    private long time;

    public DeleteOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = Operator.OperatorType.DELETE;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getTime() {
        return time;
    }

}
