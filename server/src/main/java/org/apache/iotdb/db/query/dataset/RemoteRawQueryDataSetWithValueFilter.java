package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RemoteRawQueryDataSetWithValueFilter extends RawQueryDataSetWithValueFilter {

  private List<RowRecord> cachedRowRecords = new ArrayList<>();
  private Object[] objects;
  private boolean[] isAllNull;
  /**
   * constructor of EngineDataSetWithValueFilter.
   *
   * @param paths paths in List structure
   * @param dataTypes time series data type
   * @param timeGenerator EngineTimeGenerator object
   * @param readers readers in List(IReaderByTimeStamp) structure
   * @param cached
   * @param ascending specifies how the data should be sorted,'True' means read in ascending time
   */
  public RemoteRawQueryDataSetWithValueFilter(
      List<PartialPath> paths,
      List<TSDataType> dataTypes,
      TimeGenerator timeGenerator,
      List<IReaderByTimestamp> readers,
      List<Boolean> cached,
      boolean ascending) {
    super(paths, dataTypes, timeGenerator, readers, cached, ascending);
  }

  /**
   * Cache row record
   *
   * @return if there has next row record.
   */
  private boolean cacheRowRecord() throws IOException {
    int cachedTimeCnt = 0;
    long[] cachedTimeArray = new long[MAX_TIME_NUM];
    // TODO: LIMIT constraint
    while (timeGenerator.hasNext() && cachedTimeCnt < MAX_TIME_NUM) {
      // 1. fill time array from time Generator
      cachedTimeArray[cachedTimeCnt++] = timeGenerator.next();
    }
    for (int i = 0; i < seriesReaderByTimestampList.size(); i++) {
      // 2. fetch results of each time series from readers using time array
      Object[] results = seriesReaderByTimestampList.get(i).getValueInTimestamps(cachedTimeArray);
      // 3. use values in results to fill row record
      for (int j = 0; j < MAX_TIME_NUM; j++) {
        if (i == 0) {
          RowRecord rowRecord = new RowRecord(cachedTimeArray[]);
        }
        fillRowRecord();
        if (results[j] != null) {
          isAllNull = false;
        }
      }
    }
    // 4. remove rowRecord if all values in one timestamp are null
    removeNonExistRecord();
  }
}
