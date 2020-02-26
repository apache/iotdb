package org.apache.iotdb.tsfile;

import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class TempTest {

  private static void initializeTsFile() throws IOException, WriteProcessException {
    TsFileWriter writer;
    String filePath = "file.ts";
    TSDataType dataType = TSDataType.FLOAT;
    TSEncoding encoding = TSEncoding.RLE;
    int lineNumber = 100000, deviceNumber = 3, sensorNumberPerDevice = 3;

    String DEVICE_PRIFIX = "d", SENSOR_PRIFIX = "s";

    FSFactory fsFactory = FSFactoryProducer.getFSFactory();


    File f = new File(filePath);
    if (f.exists()) f.delete();

    // initialize writer and set table structure
    writer = new TsFileWriter(fsFactory.getFile("data.tsfile"));
    for (int i = 0; i < sensorNumberPerDevice; i++) {
      MeasurementSchema descriptor = new MeasurementSchema(SENSOR_PRIFIX + i, dataType, encoding);
      writer.addMeasurement(descriptor);
    }

    // write data
    for (int i = 0; i < lineNumber; i++) {
      for (int j = 0; j < deviceNumber; j++) {
        TSRecord record = new TSRecord(i, DEVICE_PRIFIX + j);
        for (int k = 0; k < sensorNumberPerDevice; k++)
          record.addTuple(new FloatDataPoint(SENSOR_PRIFIX + k, (float) Math.random()));
        writer.write(record);
      }
    }
    writer.close();
  }

  private static void readData() throws IOException {

    File f = new File("out.csv");
    if(!f.exists()){
      f.createNewFile();
    }else {
      f.delete();
      f.createNewFile();
    }
    FileWriter fw = new FileWriter(f, true);

    TsFileSequenceReader reader = new TsFileSequenceReader("data.tsfile");
    ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d2.s1"));


    Filter sFilter = ValueFilter.gt(0.5F);
    SingleSeriesExpression s = new SingleSeriesExpression(new Path("d2.s1"),
            sFilter);

    Filter timeFilter = TimeFilter.lt(4838L);
    IExpression t = new GlobalTimeExpression(timeFilter);
    IExpression expression = BinaryExpression.or(s, t);
    QueryExpression q1 = QueryExpression.create(paths, expression);
    QueryExpression q2 = QueryExpression.create(paths, BinaryExpression.and(s, t));
    QueryExpression q3 = QueryExpression.create(paths, t);
    QueryExpression q4 = QueryExpression.create(paths, s);

    QueryExpression[] queryExpressions = {q1, q2, q3, q4};

    for(QueryExpression q : queryExpressions){
      long sum = 0;
      int repeat = 10000;
      for(int i = 0; i < repeat; i++){
        sum += runTime(readTsFile, q);
      }
      fw.write("" +sum/repeat+"\n");
    }
    fw.close();
    reader.close();
  }

  public static long runTime(ReadOnlyTsFile readOnlyTsFile, QueryExpression queryExpression) throws IOException {
    long start = System.nanoTime();
    QueryDataSet queryDataSet = readOnlyTsFile.query(queryExpression);
    while (queryDataSet.hasNext()){
      queryDataSet.next();
    }
    return System.nanoTime() - start;
  }


  public static void main(String args[]) throws IOException, WriteProcessException {
//      initializeTsFile();

    readData();

  }


}
