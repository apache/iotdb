package org.apache.iotdb.db.exception.metadata;

import org.apache.iotdb.rpc.TSStatusCode;

/**
 * @author zhanghang
 * @date 2021/10/22 18:16
 */
public class NoTemplateOnMNodeException extends MetadataException {

  public NoTemplateOnMNodeException(String path) {
    super(
        String.format("NO template on " + path),
        TSStatusCode.NO_TEMPLATE_ON_MNODE.getStatusCode(),
        true);
  }
}
