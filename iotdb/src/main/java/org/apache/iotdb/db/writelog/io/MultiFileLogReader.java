package org.apache.iotdb.db.writelog.io;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public class MultiFileLogReader implements ILogReader {

  private SingleFileLogReader currentReader;
  private File[] files;
  private int fileIdx = 0;

  public MultiFileLogReader(File[] files) {
    this.files = files;
  }

  @Override
  public void close() {
    if (currentReader != null) {
      currentReader.close();
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    if (files == null) {
      return false;
    }
    if (currentReader == null) {
      currentReader = new SingleFileLogReader(files[fileIdx++]);
    }
    if (currentReader.hasNext()) {
      return true;
    }
    while (fileIdx < files.length) {
      currentReader.open(files[fileIdx++]);
      if (currentReader.hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public PhysicalPlan next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return currentReader.next();
  }
}
