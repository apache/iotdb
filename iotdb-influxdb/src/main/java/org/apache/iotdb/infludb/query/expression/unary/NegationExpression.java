package org.apache.iotdb.infludb.query.expression.unary;

import org.apache.iotdb.infludb.query.expression.Expression;

public class NegationExpression implements Expression {
    protected Expression expression;

    public NegationExpression(Expression expression) {
        this.expression = expression;
    }

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }
}
