package org.apache.iotdb.infludb.query.expression.unary;

import org.apache.iotdb.infludb.query.expression.Expression;

public class NodeExpression implements Expression {
    protected String name;

    public NodeExpression(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
