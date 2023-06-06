package org.apache.iotdb.db.exception.metadata.view;

public class ViewContainsAggregationException extends UnsupportedViewException {

  private static final String VIEW_CONTAINS_AGGREGATION_FUNCTION =
      "This view contains aggregation function(s) named [%s]";

  public ViewContainsAggregationException(String namesOfAggregationFunctions) {
    super(String.format(VIEW_CONTAINS_AGGREGATION_FUNCTION, namesOfAggregationFunctions), true);
  }
}
