package org.apache.iotdb.tsfile.fileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

public class IoTDBFile extends File {

  private Path hdfsPath;

  public IoTDBFile(String pathname) {
    super(pathname);
    if (TSFileConfig.isHdfsStorage) {
      hdfsPath = new Path(pathname);
    }
  }

  public IoTDBFile(String parent, String child) {
    super(parent, child);
  }

  public IoTDBFile(File parent, String child) {
    super(parent, child);
  }

  public IoTDBFile(URI uri) {
    super(uri);
  }

  @Override
  public String getAbsolutePath() {
    if (TSFileConfig.isHdfsStorage) {
      return hdfsPath.toUri().toString();
    }
    return super.getAbsolutePath();
  }

  @Override
  public long length() {
    if (TSFileConfig.isHdfsStorage) {
      try {
        FileSystem fs = hdfsPath.getFileSystem(new Configuration());
        return fs.getFileStatus(hdfsPath).getLen();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return super.length();
  }

  @Override
  public boolean exists() {
    if (TSFileConfig.isHdfsStorage) {
      try {
        FileSystem fs = hdfsPath.getFileSystem(new Configuration());
        return fs.exists(hdfsPath);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return super.exists();
  }

  public IoTDBFile[] listFiles() {
    if (TSFileConfig.isHdfsStorage) {
      ArrayList<IoTDBFile> files = new ArrayList<>();
      try {
        FileSystem fs = hdfsPath.getFileSystem(new Configuration());
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(hdfsPath, true);
        while (iterator.hasNext()) {
          LocatedFileStatus fileStatus = iterator.next();
          Path fullPath = fileStatus.getPath();
          files.add(new IoTDBFile(fullPath.toUri()));
        }
        return files.toArray(new IoTDBFile[files.size()]);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    File[] rawFiles = super.listFiles();
    IoTDBFile[] files = new IoTDBFile[rawFiles.length];
    for (int i = 0; i < rawFiles.length; i++) {
      files[i] = new IoTDBFile(rawFiles[i].getPath());
    }
    return files;
  }

  public IoTDBFile[] listFiles(FileFilter filter) {
    if (TSFileConfig.isHdfsStorage) {
      ArrayList<IoTDBFile> files = new ArrayList<>();
      try {
        PathFilter pathFilter = new GlobFilter(filter.toString()); // TODO
        FileSystem fs = hdfsPath.getFileSystem(new Configuration());
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(hdfsPath, true);
        while (iterator.hasNext()) {
          LocatedFileStatus fileStatus = iterator.next();
          Path fullPath = fileStatus.getPath();
          if (pathFilter.accept(fullPath)) {
            files.add(new IoTDBFile(fullPath.toUri()));
          }
        }
        return files.toArray(new IoTDBFile[files.size()]);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    File[] rawFiles = super.listFiles(filter);
    IoTDBFile[] files = new IoTDBFile[rawFiles.length];
    for (int i = 0; i < rawFiles.length; i++) {
      files[i] = new IoTDBFile(rawFiles[i].getPath());
    }
    return files;
  }

  public IoTDBFile getParentFile() {
    if (TSFileConfig.isHdfsStorage) {
      return new IoTDBFile(hdfsPath.getParent().toUri());
    }
    return new IoTDBFile(super.getParentFile().getPath());

  }

  @Override
  public boolean createNewFile() throws IOException {
    if (TSFileConfig.isHdfsStorage) {
      FileSystem fs = hdfsPath.getFileSystem(new Configuration());
      return fs.createNewFile(hdfsPath);
    }
    return super.createNewFile();
  }

  @Override
  public boolean delete() {
    if (TSFileConfig.isHdfsStorage) {
      try {
        FileSystem fs = hdfsPath.getFileSystem(new Configuration());
        return fs.delete(hdfsPath, true);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return super.delete();
  }

  @Override
  public boolean mkdirs() {
    if (TSFileConfig.isHdfsStorage) {
      try {
        FileSystem fs = hdfsPath.getFileSystem(new Configuration());
        return fs.mkdirs(hdfsPath);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return super.mkdirs();
  }
}
