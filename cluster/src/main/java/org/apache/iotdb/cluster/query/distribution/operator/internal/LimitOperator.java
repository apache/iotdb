package org.apache.iotdb.cluster.query.distribution.operator.internal;

import org.apache.iotdb.cluster.query.distribution.common.Tablet;

/**
 * LimitOperator is used to select top n result. It uses the default order of upstream operators
 *
 * <p>Children type: [All the operators whose result set is Tablet]
 */
public class LimitOperator extends InternalOperator<Tablet> {

  // The limit count
  private int limit;

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public Tablet getNextBatch() {
    return null;
  }
}
