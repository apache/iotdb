package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.metadata.PartialPath;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class SelectAggrOperator extends SelectOperator {

  private List<String> aggregations = new ArrayList<>();

  /** init with tokenIntType, default operatorType is <code>OperatorType.SELECT</code>. */
  public SelectAggrOperator(ZoneId zoneId) {
    super(zoneId);
  }

  public void addClusterPath(PartialPath suffixPath, String aggregation) {
    suffixList.add(suffixPath);
    if (aggregations == null) {
      aggregations = new ArrayList<>();
    }
    aggregations.add(aggregation);
  }

  public List<String> getAggregations() {
    return this.aggregations;
  }

  public void setAggregations(List<String> aggregations) {
    this.aggregations = aggregations;
  }

  public boolean hasAggregation() {
    return true; // todo: hasBuiltinAggregation || hasUDAF
  }
}
