package org.apache.iotdb.cluster.query.reader.mult;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class MultSeriesRawDataPointReader implements IMultPointReader {
  private Map<String, IPointReader> partitalPathReaders;

  public MultSeriesRawDataPointReader(Map<String, IPointReader> partitalPathReaders) {
    this.partitalPathReaders = partitalPathReaders;
  }

  @Override
  public boolean hasNextTimeValuePair(String fullPath) throws IOException {
    IPointReader seriesRawDataPointReader = partitalPathReaders.get(fullPath);
    return seriesRawDataPointReader.hasNextTimeValuePair();
  }

  @Override
  public TimeValuePair nextTimeValuePair(String fullPath) throws IOException {
    IPointReader seriesRawDataPointReader = partitalPathReaders.get(fullPath);
    return seriesRawDataPointReader.nextTimeValuePair();
  }

  @Override
  public Set<String> getAllPaths() {
    return partitalPathReaders.keySet();
  }

  @Override
  public boolean hasNextTimeValuePair() throws IOException {
    return false;
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    return null;
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {}
}
