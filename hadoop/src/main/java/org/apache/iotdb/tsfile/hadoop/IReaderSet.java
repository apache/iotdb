package org.apache.iotdb.tsfile.hadoop;

import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.util.List;

public interface IReaderSet {

  void setReader(TsFileSequenceReader reader);

  void setMeasurementIds(List<String> measurementIds);

  void setReadDeviceId(boolean isReadDeviceId);

  void setReadTime(boolean isReadTime);
}
