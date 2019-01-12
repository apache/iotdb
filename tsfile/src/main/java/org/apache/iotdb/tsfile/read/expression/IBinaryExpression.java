package org.apache.iotdb.tsfile.read.expression;

/**
 * @author Jinrui Zhang
 */
public interface IBinaryExpression extends IExpression {
    IExpression getLeft();

    IExpression getRight();


}
