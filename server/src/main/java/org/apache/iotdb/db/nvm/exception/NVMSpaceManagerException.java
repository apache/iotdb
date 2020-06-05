package org.apache.iotdb.db.nvm.exception;

import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.rpc.TSStatusCode;

public class NVMSpaceManagerException extends IoTDBException {

  private static final long serialVersionUID = 3502239072309147687L;

  public NVMSpaceManagerException(String message) {
    super(message, TSStatusCode.NVMSPACE_MANAGER_ERROR.getStatusCode());
  }
}
