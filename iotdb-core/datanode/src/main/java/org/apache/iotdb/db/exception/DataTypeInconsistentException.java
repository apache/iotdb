package org.apache.iotdb.db.exception;

import org.apache.tsfile.enums.TSDataType;

public class DataTypeInconsistentException extends WriteProcessException {

  public DataTypeInconsistentException(TSDataType existing, TSDataType incoming) {
    super(
        String.format(
            "Inconsistent data types, existing data type: %s, incoming: %s", existing, incoming));
  }
}
