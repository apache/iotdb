package cn.edu.tsinghua.iotdb.qp.logical;

/**
 * RootOperator indicates the operator that could be executed as a entire command. RootOperator
 * consists of SFWOperator, like INSERT/UPDATE/DELETE, and other Operators.
 *
 * @author kangrong
 * @author qiaojialin
 */
public abstract class RootOperator extends Operator {

    public RootOperator(int tokenIntType) {
        super(tokenIntType);
    }
    
}
