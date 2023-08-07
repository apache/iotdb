package org.apache.iotdb;

import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SessionInsert {
  private static final Logger logger = LoggerFactory.getLogger(SessionInsert.class);

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    File[] filesShouldDelete =
        new File[] {
          new File("/wal/_0-0-1.wal"),
          new File("/wal/_1-0-1.wal"),
          new File("/wal/_2-0-1.wal"),
          new File("/wal/_3-0-1.wal")
        };
    List<String> successfullyDeleted = Arrays.asList("/wal/_0.wal", "/wal/_1.wal");
    StringBuilder summary =
        new StringBuilder(
            String.format(
                "wal node-%s delete outdated files summary:the range that should be removed is: [%d,%d] ,end file index is: [%s].The following reasons influenced the result: %s",
                "1",
                WALFileUtils.parseVersionId(filesShouldDelete[0].getName()),
                WALFileUtils.parseVersionId(
                    filesShouldDelete[filesShouldDelete.length - 1].getName()),
                88,
                System.getProperty("line.separator")));

    if (true) {
      summary
          .append("- MemTable has been flushed but pinned by PIPE,the MemTableId list is : ")
          .append(StringUtils.join(Arrays.asList(1, 2, 3, 4), ","))
          .append(".")
          .append(System.getProperty("line.separator"));
    }
    if (true) {
      summary.append(
          String.format(
              "- The data in the wal file was not consumed by the consensus group.current search index is %d,safely delete index is %d",
              88, 99));
    }
    logger.info(summary.toString());
  }

  private static void insertRecord()
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    String deviceId = "root.sg.d1";
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 2000000; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
    }
  }
}
