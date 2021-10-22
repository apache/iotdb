package org.apache.iotdb.db.exception.metadata;

import org.apache.iotdb.rpc.TSStatusCode;

/**
 * @author zhanghang
 * @date 2021/10/22 18:24
 */
public class DifferentTemplateException extends MetadataException {

  public DifferentTemplateException(String path, String templateName) {
    super(
        String.format("The template on " + path + " is different from " + templateName),
        TSStatusCode.DIFFERENT_TEMPLATE.getStatusCode(),
        true);
  }
}
