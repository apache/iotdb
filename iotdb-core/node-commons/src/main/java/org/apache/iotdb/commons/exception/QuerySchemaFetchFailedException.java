package org.apache.iotdb.commons.exception;

/**
 * Failed to fetch schema via Coordinator during query execution.
 * This is likely caused by network partition.
 */
public class QuerySchemaFetchFailedException extends IoTDBRuntimeException{
  public QuerySchemaFetchFailedException(String message, int errorCode) {
    super(message, errorCode);
  }
}
