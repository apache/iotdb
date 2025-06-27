package org.apache.iotdb.udf.api.exception;

public class ExecutionFailedInExternalDB extends UDFException {
  public ExecutionFailedInExternalDB(String externalDB, Throwable throwable) {
    super(String.format("Execution failed in %s.", externalDB), throwable);
  }
}
