package org.apache.iotdb.tsfile.experiment;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.*;
import org.apache.iotdb.tsfile.read.common.*;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

public class MultiReplicaGenerator {
  static final String oriTsFilePath = "E:\\Thing\\Workspace\\IoTDB\\res\\sequence.tsfile";
  static final String deviceName = "root.test.device";
  static final String physicalConfFile = "E:\\Thing\\Workspace\\IoTDB\\res\\DivergentDesign_3R.txt";
  static final String generatedTsFilePattern = "E:\\Thing\\Workspace\\IoTDB\\res\\divergentDesign%d.tsfile";
  static final int measureCntPerChunkGroup = 10000;
  static final int replicaCount = 3;

  public static void main(String[] args) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(oriTsFilePath);
         ReadOnlyTsFile tsFile = new ReadOnlyTsFile(reader)) {
      for (int replicaIdx = 0; replicaIdx < replicaCount; ++replicaIdx) {
        List<Path> pathList = new ArrayList<>();
        for (int i = 0; i < 1000; ++i) {
          pathList.add(new Path(deviceName, "s" + i));
        }
        List<Integer> orderList = readOrder(replicaIdx);
        Schema schema = new Schema();
        List<MeasurementSchema> schemaList = new ArrayList<>();
        for (int i = 0; i < 1000; ++i) {
          schemaList.add(new MeasurementSchema("s" + orderList.get(i), TSDataType.DOUBLE, TSEncoding.GORILLA));
          schema.registerTimeseries(pathList.get(orderList.get(i)),
                  new MeasurementSchema("s" + orderList.get(i), TSDataType.DOUBLE, TSEncoding.GORILLA));
        }
        QueryExpression expression = QueryExpression.create(pathList, null);
        QueryDataSet dataset = tsFile.query(expression);
        TsFileWriter writer = getWriter(replicaIdx, schema);
        Tablet tablet = new Tablet(deviceName, schemaList, measureCntPerChunkGroup);
        while (dataset.hasNext()) {
          long[] timestamps = tablet.timestamps;
          Object[] values = tablet.values;
          for (int i = 0; i < measureCntPerChunkGroup && dataset.hasNext(); ++i) {
            RowRecord record = dataset.next();
            List<Field> fields = record.getFields();
            int rowNum = tablet.rowSize++;
            timestamps[rowNum] = record.getTimestamp();
            for (int j = 0; j < 1000; ++j) {
              double[] sensor = (double[]) values[j];
              sensor[rowNum] = fields.get(orderList.get(j)).getDoubleV();
            }
          }
          writer.write(tablet);
          writer.flushAllChunkGroups(tablet.getMeasurementIndex());
          tablet.reset();
        }
        writer.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static List<Integer> readOrder(int replicaIdx) {
    File file = new File(physicalConfFile);
    byte[] buffer = new byte[(int) file.length()];
    try (FileInputStream stream = new FileInputStream(file)) {
      stream.read(buffer);
      String dataStr = new String(buffer);
      String[] configurations = dataStr.split("\n\n\n");
      String[] confAndWorkload = configurations[replicaIdx].split("\n");
      String conf = confAndWorkload[0];
      String[] measurements = conf.split(" ");
      List<Integer> order = new ArrayList<>();
      for (String measurement : measurements) {
        order.add(Integer.valueOf(measurement.substring(1)));
      }
      return order;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  static TsFileWriter getWriter(int replicaIdx, Schema schema) {
    File file = FSFactoryProducer.getFSFactory().getFile(String.format(generatedTsFilePattern, replicaIdx));
    try {
      return new TsFileWriter(file, schema);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}
