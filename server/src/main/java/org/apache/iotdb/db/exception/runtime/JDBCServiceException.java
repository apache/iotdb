package org.apache.iotdb.db.exception.runtime;

public class JDBCServiceException extends RuntimeException{

  private static final long serialVersionUID = 520836932066897810L;

  public JDBCServiceException(String message) {
    super(message);
  }

  public JDBCServiceException(String message, Throwable e) {
    super(message + e.getMessage());
  }

}
