package org.apache.iotdb.db.qp.physical.crud;


import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator;

import java.util.ArrayList;
import java.util.List;

public class AggregationPlan extends QueryPlan {

    private List<String> aggregations = new ArrayList<>();

    public AggregationPlan() {
        super();
        setOperatorType(Operator.OperatorType.AGGREGATION);
    }

    @Override
    public List<String> getAggregations() {
        return aggregations;
    }

    public void setAggregations(List<String> aggregations) {
        this.aggregations = aggregations;
    }
}
