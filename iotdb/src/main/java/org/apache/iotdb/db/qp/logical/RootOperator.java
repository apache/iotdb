package org.apache.iotdb.db.qp.logical;

/**
 * RootOperator indicates the operator that could be executed as a entire command. RootOperator
 * consists of SFWOperator, like INSERT/UPDATE/DELETE, and other Operators.
 */
public abstract class RootOperator extends Operator {

    public RootOperator(int tokenIntType) {
        super(tokenIntType);
    }
    
}
