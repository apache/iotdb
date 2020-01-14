package org.apache.iotdb.db.nvm.exception;

import org.apache.iotdb.db.exception.ProcessException;
import org.apache.iotdb.rpc.TSStatusCode;

public class NVMSpaceManagerException extends ProcessException {

  private static final long serialVersionUID = 3502239072309147687L;

  public NVMSpaceManagerException(String message) {
    super(message);
    errorCode = TSStatusCode.NVMSPACE_MANAGER_EROOR.getStatusCode();
  }
}
