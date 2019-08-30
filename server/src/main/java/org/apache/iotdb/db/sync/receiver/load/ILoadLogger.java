package org.apache.iotdb.db.sync.receiver.load;

import java.io.File;
import java.io.IOException;

public interface ILoadLogger {

  void startLoadDeletedFiles() throws IOException;

  void finishLoadDeletedFile(File file) throws IOException;

  void startLoadTsFiles() throws IOException;

  void finishLoadTsfile(File file) throws IOException;

  void close() throws IOException;

}
