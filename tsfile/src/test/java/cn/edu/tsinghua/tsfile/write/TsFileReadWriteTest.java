package cn.edu.tsinghua.tsfile.write;

import cn.edu.tsinghua.tsfile.exception.write.WriteProcessException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.read.ReadOnlyTsFile;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.write.record.datapoint.*;
import cn.edu.tsinghua.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class TsFileReadWriteTest {
    private String path = "read_write_rle.tsfile";
    private File f;
    private TsFileWriter tsFileWriter;
    private final double delta = 0.0000001;


    @Before
    public void setUp() throws Exception {
        f = new File(path);
        if (f.exists()) {
            f.delete();
        }
        tsFileWriter = new TsFileWriter(f);
    }

    @After
    public void tearDown() throws Exception {
        f = new File(path);
        if (f.exists()) {
            f.delete();
        }
    }

    @Test
    public void intTest() throws IOException, WriteProcessException {
        int floatCount = 1024*1024*13+1023;
        // add measurements into file schema
        tsFileWriter.addMeasurement(new MeasurementSchema("sensor_1", TSDataType.INT32, TSEncoding.RLE));
        for (long i = 1; i < floatCount; i++) {
            // construct TSRecord
            TSRecord tsRecord = new TSRecord(i, "device_1");
            DataPoint dPoint1 = new IntDataPoint("sensor_1", (int)i);
            tsRecord.addTuple(dPoint1);
            // write a TSRecord to TsFile
            tsFileWriter.write(tsRecord);
        }
        // close TsFile
        tsFileWriter.close();
        TsFileSequenceReader reader = new TsFileSequenceReader(path);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        QueryExpression queryExpression = QueryExpression.create(paths, null);

        QueryDataSet queryDataSet = readTsFile.query(queryExpression);
        for(int j = 0; j< paths.size(); j++) {
            assertEquals(paths.get(j), queryDataSet.getPaths().get(j));
        }

        int i = 1;
        while (queryDataSet.hasNext()) {
            RowRecord r = queryDataSet.next();
            assertEquals(i, r.getTimestamp());
            assertEquals(i, r.getFields().get(0).getIntV());
            i++;
        }
        reader.close();
    }

    @Test
    public void longTest() throws IOException, WriteProcessException {
        int floatCount = 1024*1024*13+1023;
        // add measurements into file schema
        tsFileWriter.addMeasurement(new MeasurementSchema("sensor_1", TSDataType.INT64, TSEncoding.RLE));
        for (long i = 1; i < floatCount; i++) {
            // construct TSRecord
            TSRecord tsRecord = new TSRecord(i, "device_1");
            DataPoint dPoint1 = new LongDataPoint("sensor_1", i);
            tsRecord.addTuple(dPoint1);
            // write a TSRecord to TsFile
            tsFileWriter.write(tsRecord);
        }
        // close TsFile
        tsFileWriter.close();
        TsFileSequenceReader reader = new TsFileSequenceReader(path);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        QueryExpression queryExpression = QueryExpression.create(paths, null);

        QueryDataSet queryDataSet = readTsFile.query(queryExpression);
        for(int j = 0; j< paths.size(); j++) {
            assertEquals(paths.get(j), queryDataSet.getPaths().get(j));
        }

        int i = 1;
        while (queryDataSet.hasNext()) {
            RowRecord r = queryDataSet.next();
            assertEquals(i, r.getTimestamp());
            assertEquals(i, r.getFields().get(0).getLongV());
            i++;
        }
        reader.close();
    }

    @Test
    public void floatTest() throws IOException, WriteProcessException {
        int floatCount = 1024*1024*13+1023;
        // add measurements into file schema
        tsFileWriter.addMeasurement(new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
        for (long i = 1; i < floatCount; i++) {
            // construct TSRecord
            TSRecord tsRecord = new TSRecord(i, "device_1");
            DataPoint dPoint1 = new FloatDataPoint("sensor_1", (float)i);
            tsRecord.addTuple(dPoint1);
            // write a TSRecord to TsFile
            tsFileWriter.write(tsRecord);
        }
        // close TsFile
        tsFileWriter.close();
        TsFileSequenceReader reader = new TsFileSequenceReader(path);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        QueryExpression queryExpression = QueryExpression.create(paths, null);

        QueryDataSet queryDataSet = readTsFile.query(queryExpression);
        for(int j = 0; j< paths.size(); j++) {
            assertEquals(paths.get(j), queryDataSet.getPaths().get(j));
        }

        int i = 1;
        while (queryDataSet.hasNext()) {
            RowRecord r = queryDataSet.next();
            assertEquals(i, r.getTimestamp());

            assertEquals((float) i, r.getFields().get(0).getFloatV(), delta);
            i++;
        }
        reader.close();
    }

    @Test
    public void doubleTest() throws IOException, WriteProcessException {
        int floatCount = 1024*1024*13+1023;
        // add measurements into file schema
        tsFileWriter.addMeasurement(new MeasurementSchema("sensor_1", TSDataType.DOUBLE, TSEncoding.RLE));
        for (long i = 1; i < floatCount; i++) {
            // construct TSRecord
            TSRecord tsRecord = new TSRecord(i, "device_1");
            DataPoint dPoint1 = new DoubleDataPoint("sensor_1", (double)i);
            tsRecord.addTuple(dPoint1);
            // write a TSRecord to TsFile
            tsFileWriter.write(tsRecord);
        }
        // close TsFile
        tsFileWriter.close();
        TsFileSequenceReader reader = new TsFileSequenceReader(path);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        QueryExpression queryExpression = QueryExpression.create(paths, null);

        QueryDataSet queryDataSet = readTsFile.query(queryExpression);
        for(int j = 0; j< paths.size(); j++) {
            assertEquals(paths.get(j), queryDataSet.getPaths().get(j));
        }

        int i = 1;
        while (queryDataSet.hasNext()) {
            RowRecord r = queryDataSet.next();
            assertEquals(i, r.getTimestamp());
            assertEquals((double) i, r.getFields().get(0).getDoubleV(), delta);
            i++;
        }
        reader.close();
    }

}
