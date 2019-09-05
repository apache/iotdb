package org.apache.iotdb.tsfile.fileSystem;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

import java.io.File;
import java.net.URI;

public enum IoTDBFileFactory {

  INSTANCE;

  public File getIoTDBFile(String pathname) {
    if (TSFileConfig.storageFs.equals("HDFS")) {
      return new HdfsFile(pathname);
    } else {
      return new File(pathname);
    }
  }

  public File getIoTDBFile(String parent, String child) {
    if (TSFileConfig.storageFs.equals("HDFS")) {
      return new HdfsFile(parent, child);
    } else {
      return new File(parent, child);
    }
  }

  public File getIoTDBFile(File parent, String child) {
    if (TSFileConfig.storageFs.equals("HDFS")) {
      return new HdfsFile(parent, child);
    } else {
      return new File(parent, child);
    }
  }

  public File getIoTDBFile(URI uri) {
    if (TSFileConfig.storageFs.equals("HDFS")) {
      return new HdfsFile(uri);
    } else {
      return new File(uri);
    }
  }

}
