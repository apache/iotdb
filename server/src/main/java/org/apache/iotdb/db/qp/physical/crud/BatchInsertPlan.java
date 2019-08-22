package org.apache.iotdb.db.qp.physical.crud;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.service.rpc.thrift.IoTDBDataType;
import org.apache.iotdb.service.rpc.thrift.TSDataValueList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

public class BatchInsertPlan extends PhysicalPlan {

  private String deviceId;
  private String[] measurements;
  private TSDataType[] dataTypes;
  private Long[] times;
  private TSDataValueList[] columns;
  private int rowCount = 0;
  private Long maxTime = null;
  private Long minTime = null;

  public BatchInsertPlan() {
    super(false, OperatorType.BATCHINSERT);
  }

  public BatchInsertPlan(String deviceId, String[] measurements, List<IoTDBDataType> dataTypes) {
    super(false, OperatorType.BATCHINSERT);
    this.deviceId = deviceId;
    this.measurements = measurements;
    setDataTypes(dataTypes);
  }

  public BatchInsertPlan(String deviceId, String[] measurements, IoTDBDataType[] dataTypes) {
    super(false, OperatorType.BATCHINSERT);
    this.deviceId = deviceId;
    this.measurements = measurements;
    setDataTypes(dataTypes);
  }


  @Override
  public List<Path> getPaths() {
    List<Path> ret = new ArrayList<>();

    for (String m : measurements) {
      ret.add(new Path(deviceId, m));
    }
    return ret;
  }

  @Override
  public void serializeTo(ByteBuffer buffer) {
    int type = PhysicalPlanType.BATCHINSERT.ordinal();
    buffer.put((byte) type);

    putString(buffer, deviceId);

    buffer.putInt(measurements.length);
    for (String m : measurements) {
      putString(buffer, m);
    }

    for (TSDataType dataType: dataTypes) {
      buffer.putShort(dataType.serialize());
    }

    buffer.putInt(times.length);
    for (Long time: times) {
      buffer.putLong(time);
    }

    for (int i = 0; i < measurements.length; i++) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      TTransport transport = new TIOStreamTransport(out);
      TBinaryProtocol tp = new TBinaryProtocol(transport);
      try {
        columns[i].write(tp);
      } catch (TException e) {
        throw new RuntimeException("meet error when serializing BatchInsertPlan", e);
      }
      byte[] bytes = out.toByteArray();
      buffer.putInt(bytes.length);
      buffer.put(bytes);
    }
  }

  @Override
  public void deserializeFrom(ByteBuffer buffer) {
    this.deviceId = readString(buffer);

    int measurementSize = buffer.getInt();
    this.measurements = new String[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      measurements[i] = readString(buffer);
    }

    this.dataTypes = new TSDataType[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      dataTypes[i] = TSDataType.deserialize(buffer.getShort());
    }

    int rows = buffer.getInt();
    this.times = new Long[rows];
    for (int i = 0; i < rows; i++) {
      this.times[i] = buffer.getLong();
    }

    this.columns = new TSDataValueList[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      int size = buffer.getInt();
      byte[] bytes = new byte[size];
      buffer.get(bytes);
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      TTransport transport = new TIOStreamTransport(bis);
      TBinaryProtocol tp = new TBinaryProtocol(transport);
      TSDataValueList tsDataValueList = new TSDataValueList();
      try {
        tsDataValueList.read(tp);
      } catch (TException e) {
        throw new RuntimeException("meet error when deserializing BatchInsertPlan", e);
      }
      columns[i] = tsDataValueList;
    }
  }

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public String[] getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<String> measurements) {
    this.measurements = new String[measurements.size()];
    measurements.toArray(this.measurements);
  }

  public void setMeasurements(String[] measurements) {
    this.measurements = measurements;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<IoTDBDataType> dataTypes) {
    this.dataTypes = new TSDataType[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      this.dataTypes[i] = QueryDataSetUtils.getTSDataTypeByIoTDBDataType(dataTypes.get(i));
    }
  }

  public void setDataTypes(IoTDBDataType[] dataTypes) {
    this.dataTypes = new TSDataType[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      this.dataTypes[i] = QueryDataSetUtils.getTSDataTypeByIoTDBDataType(dataTypes[i]);
    }
  }

  public TSDataValueList[] getColumns() {
    return columns;
  }

  public void setColumns(List<TSDataValueList> columns) {
    this.columns = new TSDataValueList[columns.size()];
    columns.toArray(this.columns);
  }

  public long getMinTime() {
    if (minTime != null) {
      return minTime;
    }
    minTime = Long.MAX_VALUE;
    for (Long time: times) {
      if (time < minTime) {
        minTime = time;
      }
    }
    return minTime;
  }

  public long getMaxTime() {
    if (maxTime != null) {
      return maxTime;
    }
    long maxTime = Long.MIN_VALUE;
    for (Long time: times) {
      if (time > maxTime) {
        maxTime = time;
      }
    }
    return maxTime;
  }

  public Long[] getTimes() {
    return times;
  }

  public void setTimes(List<Long> times) {
    this.times = new Long[times.size()];
    times.toArray(this.times);
  }

  public void setTimes(Long[] times) {
    this.times = times;
  }

  public int getRowCount() {
    return rowCount;
  }

  public void setRowCount(int size) {
    this.rowCount = size;
  }

  public void setColumns(TSDataValueList[] columns) {
    this.columns = columns;
  }
}
