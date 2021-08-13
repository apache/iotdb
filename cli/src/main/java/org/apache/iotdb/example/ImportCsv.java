package org.apache.iotdb.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tool.AbstractCsvTool;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.tool.AbstractCsvTool.STRING_TIME_FORMAT;


public class ImportCsv {

    public static void main(String[] args) throws IOException, IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("127.0.0.1", 6667, "root", "root");
        CSVParser csvRecords = readCsvFile("C:\\Users\\12205\\Desktop\\dump0.csv");
        List<String> headerNames = csvRecords.getHeaderNames();
        List<CSVRecord> records = csvRecords.getRecords();
        HashMap<String, List<String>> deviceAndMeasurementNames = getDeviceAndMeasurementNames(headerNames);
        SimpleDateFormat timeFormatter = formatterInit(records.get(1).get("Time"));
        List<Long> times = records.stream().map(record -> {
            try {
                return timeFormatter.parse(record.get("Time")).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return null;
        }).collect(Collectors.toList());

        for (Map.Entry<String, List<String>> entry : deviceAndMeasurementNames.entrySet()) {
            String deviceId = entry.getKey();
            List<String> measurementNames = entry.getValue();
            List<List<Object>> valuesList = new ArrayList<>();
            List<List<String>> measurementsList = new ArrayList<>();
            records.stream()
                    .forEach(record -> {
                        ArrayList<Object> values = new ArrayList<>();
                        ArrayList<String> measurements = new ArrayList<>();
                        measurementNames.stream()
                                .forEach(measurementName -> {
                                    String value = record.get(deviceId + "." + measurementName);
                                    if (!value.equals("")) {
                                        measurements.add(measurementName);
                                        values.add(value);
                                    }
                                });
                        valuesList.add(values);
                        measurementsList.add(measurements);
                    });
//            session.insertRecordsOfOneDevice(deviceId, times, measurementsList, null, valuesList);
            System.out.println("device:"+deviceId);
            System.out.println("measurementsList:"+measurementsList);
            System.out.println("valuesList:"+valuesList+'\n');
        }
    }
    private static CSVParser readCsvFile(String path) throws IOException {
        return CSVFormat
                .EXCEL
                .withFirstRecordAsHeader()
                .withQuote('\'')
                .parse(new InputStreamReader(new FileInputStream(path)));
    }

    private static HashMap<String, List<String>> getDeviceAndMeasurementNames(List<String> headerNames) {
        HashMap<String, List<String>> deviceAndMeasurementNames = new HashMap<>();
        for (String headerName : headerNames) {
            if (headerName.equals("Time") || headerName.equals("device")) continue;
            String[] split = headerName.split("\\.");
            String measurementName = split[split.length - 1];
            String deviceName = headerName.replace("." + measurementName, "");
            if (!deviceAndMeasurementNames.containsKey(deviceName)) {
                deviceAndMeasurementNames.put(deviceName, new ArrayList<String>());
            }
            deviceAndMeasurementNames.get(deviceName).add(measurementName);
        }
        return deviceAndMeasurementNames;
    }

    private static SimpleDateFormat formatterInit(String time) {
        try {
            Long.parseLong(time);
            return null;
        } catch (Exception ignored) {
            // do nothing
        }

        for (String timeFormat : STRING_TIME_FORMAT) {
            SimpleDateFormat format = new SimpleDateFormat(timeFormat);
            try {
                format.parse(time).getTime();
                return format;
            } catch (java.text.ParseException ignored) {
                // do nothing
            }
        }
        return null;
    }
}
