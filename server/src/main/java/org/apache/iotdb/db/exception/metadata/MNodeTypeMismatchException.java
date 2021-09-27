package org.apache.iotdb.db.exception.metadata;

import org.apache.iotdb.db.metadata.MetadataConstant;

public class MNodeTypeMismatchException extends MetadataException {

  public MNodeTypeMismatchException(String path, byte expectedType) {
    super(
        String.format(
            "MNode [%s] is not a %s.", path, MetadataConstant.getMNodeTypeName(expectedType)));
  }
}
