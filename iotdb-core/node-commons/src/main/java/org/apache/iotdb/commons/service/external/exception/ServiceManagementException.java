package org.apache.iotdb.commons.service.external.exception;

public class ServiceManagementException extends RuntimeException {

  public ServiceManagementException(String message) {
    super(message);
  }

  public ServiceManagementException(String message, Throwable cause) {
    super(message, cause);
  }
}
