package org.apache.iotdb.db.engine.cache;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.common.Path;

public class TsResource{
  TsFileResource file;
  Path path;
  public TsResource(){

  }
  public TsResource(TsFileResource file, Path path){
    this.file = file;
    this.path = path;
  }
}
