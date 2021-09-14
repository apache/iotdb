package org.apache.iotdb.infludb.qp.logical.function;

import org.apache.iotdb.infludb.query.expression.Expression;

import java.util.List;

public abstract class Function {


    //包含了可能存在参数
    private List<Expression> expressionList;

    public Function(List<Expression> expressionList) {
        this.expressionList = expressionList;
    }

    public Function() {
    }


    //计算最后的结构
    public abstract FunctionValue calculate();

    //获取expressionList
    public List<Expression> getExpressions() {
        return this.expressionList;
    }


}
