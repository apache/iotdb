package org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils;

import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.udf.api.access.Row;

public class UDTFCaller {

  public static Object udtfCall(UDTFExecutor executor, Row row) {
    executor.execute(row);
    return executor.getCurrentValue();
  }
}
