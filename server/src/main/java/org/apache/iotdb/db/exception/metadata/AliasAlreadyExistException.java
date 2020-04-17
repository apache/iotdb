package org.apache.iotdb.db.exception.metadata;

import org.apache.iotdb.rpc.TSStatusCode;

public class AliasAlreadyExistException extends MetadataException {

  private static final long serialVersionUID = 7299770003548114589L;

  public AliasAlreadyExistException(String path, String alias) {
    super(String.format("Alias [%s] for Path [%s] already exist", alias, path),
            TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode());
  }
}
