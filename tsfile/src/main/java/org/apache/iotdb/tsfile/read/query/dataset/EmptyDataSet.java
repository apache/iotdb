package org.apache.iotdb.tsfile.read.query.dataset;

import java.util.Collections;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public class EmptyDataSet extends QueryDataSet {

  public EmptyDataSet() {
    super(Collections.emptyList(), Collections.emptyList());
  }

  @Override
  protected boolean hasNextWithoutConstraint() {
    return false;
  }

  @Override
  protected RowRecord nextWithoutConstraint() {
    return null;
  }
}
