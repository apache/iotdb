package org.apache.iotdb.rpc;

import java.lang.reflect.Proxy;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TS_Status;
import org.apache.iotdb.service.rpc.thrift.TS_StatusCode;

public class RpcUtils {

  public static TSIService.Iface newSynchronizedClient(TSIService.Iface client) {
    return (TSIService.Iface) Proxy.newProxyInstance(RpcUtils.class.getClassLoader(),
        new Class[]{TSIService.Iface.class}, new SynchronizedHandler(client));
  }

  /**
   * verify success.
   *
   * @param status -status
   */
  public static void verifySuccess(TS_Status status) throws IoTDBRPCException {
    if (status.getStatusCode() != TS_StatusCode.SUCCESS_STATUS) {
      throw new IoTDBRPCException(status.errorMessage);
    }
  }

}
