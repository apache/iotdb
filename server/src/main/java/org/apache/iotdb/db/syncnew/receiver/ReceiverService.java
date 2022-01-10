package org.apache.iotdb.db.syncnew.receiver;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.Arrays;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_PIPE_NAME;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_PIPE_START_TIME;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_PIPE_STATUS;

public class ReceiverService {
  public QueryDataSet show() {
    ListDataSet dataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_PIPE_NAME, false),
                new PartialPath(COLUMN_PIPE_STATUS, false),
                new PartialPath(COLUMN_PIPE_START_TIME, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT));
    for (int i = 0; i < 3; i++) {
      RowRecord rowRecord = new RowRecord(i);
      Field pipeName = new Field(TSDataType.TEXT);
      Field pipeStatus = new Field(TSDataType.TEXT);
      Field pipeStartTime = new Field(TSDataType.TEXT);
      pipeName.setBinaryV(new Binary("pipe" + i));
      pipeStatus.setBinaryV(new Binary("RUNNING"));
      pipeStartTime.setBinaryV(new Binary("2021-01-10 10:55:55"));
      rowRecord.addField(pipeName);
      rowRecord.addField(pipeStatus);
      rowRecord.addField(pipeStartTime);
      dataSet.putRecord(rowRecord);
    }
    return dataSet;
  }

  public static ReceiverService getInstance() {
    return ReceiverServiceHolder.INSTANCE;
  }

  private static class ReceiverServiceHolder {
    private static final ReceiverService INSTANCE = new ReceiverService();

    private ReceiverServiceHolder() {}
  }
}
