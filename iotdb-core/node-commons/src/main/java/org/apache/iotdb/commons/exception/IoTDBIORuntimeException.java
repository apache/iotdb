package org.apache.iotdb.commons.exception;

import java.io.IOException;

public class IoTDBIORuntimeException extends RuntimeException {
  public IoTDBIORuntimeException(IOException cause) {
    super(cause);
  }

  @Override
  public synchronized IOException getCause() {
    return (IOException) super.getCause();
  }
}
