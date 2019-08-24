package org.apache.iotdb.rpc;

public class IoTDBRPCException extends Exception{

  private static final long serialVersionUID = -1268775292265203036L;

  public IoTDBRPCException(String reason) {
    super(reason);
  }

}
