package org.apache.iotdb.db.query.executor;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;

public class EnumeratorDataSet extends QueryDataSet {

  private final TSDataType[] dataTypes;
  private final Enumerator<Object[]> enumerator;

  public EnumeratorDataSet(TSDataType[] dataTypes, Enumerator<Object[]> enumerator) {
    this.dataTypes = dataTypes;
    this.enumerator = enumerator;

  }

  @Override
  public boolean hasNextWithoutConstraint() throws IOException {
    return this.enumerator.moveNext();
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    Object[] next = this.enumerator.current();
    if (next == null || next[0] == null) {
      return null;
    }
    RowRecord record = new RowRecord((Long) next[0]);
    for (int i = 0; i < this.dataTypes.length; i++) {
      TSDataType dataType = this.dataTypes[i];
      record.addField(next[i + 1], dataType);
    }
    return record;
  }

}
