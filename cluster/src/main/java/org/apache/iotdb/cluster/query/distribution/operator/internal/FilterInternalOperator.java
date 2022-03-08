package org.apache.iotdb.cluster.query.distribution.operator.internal;

import org.apache.iotdb.cluster.query.distribution.common.Tablet;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;

/**
 * (We use FilterExecOperator to distinguish itself from the FilterOperator used in single-node
 * IoTDB) The FilterExecOperator is responsible to filter the RowRecord from Tablet.
 *
 * <p>Children type: [All the operators whose result set is Tablet]
 */
public class FilterInternalOperator extends InternalOperator<Tablet> {

  // The filter
  private FilterOperator rowFilter;

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public Tablet getNextBatch() {
    return null;
  }
}
