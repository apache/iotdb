package org.apache.iotdb.db.exception.metadata;

import org.apache.iotdb.rpc.TSStatusCode;

/**
 * @author zhanghang
 * @date 2021/10/22 18:37
 */
public class TemplateIsInUseException extends MetadataException {

  public TemplateIsInUseException(String path) {
    super(
        String.format("Template is in use on " + path),
        TSStatusCode.TEMPLATE_IS_IN_USE.getStatusCode(),
        true);
  }
}
