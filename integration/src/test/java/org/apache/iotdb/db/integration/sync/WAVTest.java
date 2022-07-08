package org.apache.iotdb.db.integration.sync;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class WAVTest {
    private Session session;
    String deviceId1 = "root.gzip";
    String deviceId2 = "root.snappy";
    String deviceId3 = "root.lz4";

    public WAVTest() throws IoTDBConnectionException {
        session = new Session("127.0.0.1", 6667, "root", "root", null);
        session.open();
    }

    public void createTimeSeries() throws IoTDBConnectionException, StatementExecutionException {
        session.createTimeseries(deviceId1 + ".s1", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.GZIP);
        session.createTimeseries(deviceId2 + ".s1", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY);
        session.createTimeseries(deviceId3 + ".s1", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.LZ4);
    }

    public void deleteTimeSeries() throws IoTDBConnectionException, StatementExecutionException {
        session.deleteTimeseries(deviceId1);
        session.deleteTimeseries(deviceId2);
    }

    public void selectData() throws IOException, IoTDBConnectionException, StatementExecutionException {
        FileOutputStream outputStream = new FileOutputStream("/Users/chenyanze/Downloads/out_2021_09_09_14_36_audio.wav");
        try (SessionDataSet dataSet = session.executeQueryStatement("select s1 from root.lz4");) {
            dataSet.setFetchSize(1024); // default is 10000
            while (dataSet.hasNext()) {
                RowRecord rowRecord = dataSet.next();
                outputStream.write(rowRecord.getFields().get(0).getBinaryV().getValues());
            }
        }
        outputStream.close();
    }

    public void insertData() throws IOException, IoTDBConnectionException, StatementExecutionException {
        FileInputStream in = new FileInputStream("/Users/chenyanze/Downloads/2021_09_09_14_36_audio.wav");
        byte[] data = new byte[2048];
        int time = 0;
//        while (in.read(data) != -1) {
//            Binary dataBinary= new Binary(data);
//            session.insertRecord(deviceId1, time,
//                    Arrays.asList(new String[]{"s1"}),
//                    Arrays.asList(new TSDataType[]{TSDataType.TEXT}),
//                    dataBinary);
//            session.insertRecord(deviceId2, time,
//                    Arrays.asList(new String[]{"s1"}),
//                    Arrays.asList(new TSDataType[]{TSDataType.TEXT}),
//                    dataBinary);
//            session.insertRecord(deviceId3, time,
//                    Arrays.asList(new String[]{"s1"}),
//                    Arrays.asList(new TSDataType[]{TSDataType.TEXT}),
//                    dataBinary);
//            time++;
//        }
        while (in.read(data) != -1) {
//            String dataString= new Binary(data).getStringValue();
            String dataString = new String(data, Charset.forName("ISO_8859_1"));
//            List<String> dataString = Arrays.asList(new String(data, Charset.forName("ISO_8859_1")));
            session.insertRecord(deviceId1, time,
                    Arrays.asList(new String[]{"s1"}),
                    Arrays.asList(new TSDataType[]{TSDataType.TEXT}),
                    dataString);
            session.insertRecord(deviceId2, time,
                    Arrays.asList(new String[]{"s1"}),
                    Arrays.asList(new TSDataType[]{TSDataType.TEXT}),
                    dataString);
            session.insertRecord(deviceId3, time,
                    Arrays.asList(new String[]{"s1"}),
                    Arrays.asList(new TSDataType[]{TSDataType.TEXT}),
                    dataString);
            time++;
        }
        in.close();
    }

    public void close() throws IoTDBConnectionException {
        session.close();
    }

    public static void main(String[] args) {
        try {
            WAVTest wavTest = new WAVTest();
//            wavTest.deleteTimeSeries();
//            wavTest.createTimeSeries();
//            wavTest.insertData();
            wavTest.selectData();
            wavTest.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
