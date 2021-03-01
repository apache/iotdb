package org.apache.iotdb.db.index.usable;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is to record the index usable range for a list of short series, which corresponds to
 * the whole matching scenario.
 *
 * <p>The series path involves wildcard characters. One series is marked as "index-usable" or
 * "index-unusable".
 *
 * <p>It's not thread-safe.
 */
public class WholeMatchIndexUsability implements IIndexUsable {

  private final Set<PartialPath> unusableSeriesSet;

  WholeMatchIndexUsability() {
    this.unusableSeriesSet = new HashSet<>();
  }

  @Override
  public void addUsableRange(PartialPath fullPath, long start, long end) {
    // do nothing temporarily
  }

  @Override
  public void minusUsableRange(PartialPath fullPath, long start, long end) {
    unusableSeriesSet.add(fullPath);
  }

  @Override
  public Set<PartialPath> getUnusableRange() {
    return Collections.unmodifiableSet(this.unusableSeriesSet);
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(unusableSeriesSet.size(), outputStream);
    for (PartialPath s : unusableSeriesSet) {
      ReadWriteIOUtils.write(s.getFullPath(), outputStream);
    }
  }

  @Override
  public void deserialize(InputStream inputStream) throws IllegalPathException, IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      unusableSeriesSet.add(new PartialPath(ReadWriteIOUtils.readString(inputStream)));
    }
  }
}
