package org.apache.iotdb.udf.api.exception;

public class CloseFailedInExternalDB extends UDFException {
  public CloseFailedInExternalDB(String externalDB, Throwable throwable) {
    super(String.format("Close connection failed in %s.", externalDB), throwable);
  }
}
