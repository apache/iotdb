package org.apache.iotdb.infludb.qp.logical.crud;

import  org.apache.iotdb.infludb.qp.constant.SQLConstant;
import org.apache.iotdb.infludb.qp.logical.Operator;

public class QueryOperator extends Operator {

    protected SelectComponent selectComponent;
    protected FromComponent fromComponent;
    protected WhereComponent whereComponent;

    public QueryOperator() {
        super(SQLConstant.TOK_QUERY);
        operatorType = Operator.OperatorType.QUERY;
    }

    public SelectComponent getSelectComponent() {
        return selectComponent;
    }

    public void setSelectComponent(SelectComponent selectComponent) {
        this.selectComponent = selectComponent;
    }

    public FromComponent getFromComponent() {
        return fromComponent;
    }

    public void setFromComponent(FromComponent fromComponent) {
        this.fromComponent = fromComponent;
    }

    public WhereComponent getWhereComponent() {
        return whereComponent;
    }

    public void setWhereComponent(WhereComponent whereComponent) {
        this.whereComponent = whereComponent;
    }
}
