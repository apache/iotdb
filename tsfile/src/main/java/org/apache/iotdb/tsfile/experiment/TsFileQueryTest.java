package org.apache.iotdb.tsfile.experiment;

import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.*;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.util.ArrayList;
import java.util.List;

public class TsFileQueryTest {
  static final String filePath = "./DivergentDesign0.tsfile";

  public static void main(String[] args) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
         ReadOnlyTsFile tsFile = new ReadOnlyTsFile(reader)) {
      List<Path> pathList = new ArrayList<>();
      pathList.add(new Path("root.test.device", "s0"));
      QueryExpression expression = QueryExpression.create(pathList, null);
      QueryDataSet dataSet = tsFile.query(expression);
      double sum = 0.0;
      while (dataSet.hasNext()) {
        RowRecord record = dataSet.next();
        sum += record.getFields().get(0).getDoubleV();
      }
      System.out.println(sum);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

