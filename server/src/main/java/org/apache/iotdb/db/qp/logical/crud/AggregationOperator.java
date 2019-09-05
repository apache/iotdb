package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.qp.logical.Operator;

import java.util.ArrayList;
import java.util.List;

public class AggregationOperator extends Operator {
  private List<String> aggregations;

  public AggregationOperator(int tokenIntType) {
    super(tokenIntType);
    aggregations = new ArrayList<>();
  }


  public List<String> getAggregations() {
    return this.aggregations;
  }

  public void setAggregations(List<String> aggregations) {
    this.aggregations = aggregations;
  }

  public void addAggregation(String aggregation) {
    aggregations.add(aggregation);
  }
}
