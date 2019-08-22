package org.apache.iotdb.db.sync.receiver.load;

import java.io.File;

public class FileLoader implements IFileLoader {

  private FileLoader(){

  }

  public static FileLoader getInstance(){
    return FileLoaderHolder.INSTANCE;
  }

  @Override
  public void addDeletedFileName(String sgName, File deletedFile) {

  }

  @Override
  public void addTsfile(String sgName, File tsfile) {

  }


  private static class FileLoaderHolder{
    private static final FileLoader INSTANCE = new FileLoader();

    private static FileLoader getInstance(){
      return INSTANCE;
    }
  }
}
