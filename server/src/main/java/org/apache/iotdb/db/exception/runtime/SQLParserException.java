package org.apache.iotdb.db.exception.runtime;

public class SQLParserException extends RuntimeException{
  private static final long serialVersionUID = 3249707655860110299L;
  public SQLParserException() {
    super("Error format in SQL statement, please check whether SQL statement is correct.");
  }
  public SQLParserException(String message) {
    super(message);
  }

  public SQLParserException(String type, String message) {
    super(String.format("Unsupported type: [%s]. " + message, type));
  }
}
