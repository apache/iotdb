package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.qp.logical.Operator;

public class LastQueryPlan extends RawDataQueryPlan {

    public LastQueryPlan() {
        super();
        setOperatorType(Operator.OperatorType.LAST);
    }
}
