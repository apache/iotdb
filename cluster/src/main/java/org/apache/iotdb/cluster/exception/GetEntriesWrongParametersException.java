package org.apache.iotdb.cluster.exception;

public class GetEntriesWrongParametersException extends Exception {
  public GetEntriesWrongParametersException(long low, long high) {
    super(String.format("invalid getEntries: parameter: %s/%s ", low, high));
  }
}