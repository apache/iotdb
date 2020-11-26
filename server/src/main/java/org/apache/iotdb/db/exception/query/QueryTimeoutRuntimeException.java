package org.apache.iotdb.db.exception.query;

/**
 * This class is used to throw run time exception when query is time out.
 */
public class QueryTimeoutRuntimeException extends RuntimeException{

  public QueryTimeoutRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public QueryTimeoutRuntimeException(String message) {
    super(message);
  }

}
