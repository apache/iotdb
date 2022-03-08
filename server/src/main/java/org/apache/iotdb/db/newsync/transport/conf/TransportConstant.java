package org.apache.iotdb.db.newsync.transport.conf;

import org.apache.iotdb.rpc.RpcUtils;

public class TransportConstant {

  private TransportConstant() {}

  public static final int DATA_CHUNK_SIZE =
      Math.min(16 * 1024 * 1024, RpcUtils.THRIFT_FRAME_MAX_SIZE);

  public static final int SUCCESS_CODE = 1;
  public static final int ERROR_CODE = -1;
  public static final int REBASE_CODE = -2;
  public static final int RETRY_CODE = -3;
  public static final int CONFLICT_CODE = -4;
}
