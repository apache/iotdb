/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.session.util;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.session.template.Template;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.apache.iotdb.session.Session.MSG_UNSUPPORTED_DATA_TYPE;

public class SessionUtils {

  private static final Logger logger = LoggerFactory.getLogger(SessionUtils.class);
  private static final byte TYPE_NULL = -2;

  public static ByteBuffer getTimeBuffer(Tablet tablet) {
    ByteBuffer timeBuffer = ByteBuffer.allocate(tablet.getTimeBytesSize());
    for (int i = 0; i < tablet.rowSize; i++) {
      timeBuffer.putLong(tablet.timestamps[i]);
    }
    timeBuffer.flip();
    return timeBuffer;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static ByteBuffer getValueBuffer(Tablet tablet) {
    ByteBuffer valueBuffer = ByteBuffer.allocate(tablet.getTotalValueOccupation());
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      MeasurementSchema schema = tablet.getSchemas().get(i);
      getValueBufferOfDataType(schema.getType(), tablet, i, valueBuffer);
    }
    if (tablet.bitMaps != null) {
      for (BitMap bitMap : tablet.bitMaps) {
        boolean columnHasNull = bitMap != null && !bitMap.isAllUnmarked();
        valueBuffer.put(BytesUtils.boolToByte(columnHasNull));
        if (columnHasNull) {
          byte[] bytes = bitMap.getByteArray();
          for (int j = 0; j < tablet.rowSize / Byte.SIZE + 1; j++) {
            valueBuffer.put(bytes[j]);
          }
        }
      }
    }
    valueBuffer.flip();
    return valueBuffer;
  }

  public static ByteBuffer getValueBuffer(List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException {
    ByteBuffer buffer = ByteBuffer.allocate(SessionUtils.calculateLength(types, values));
    SessionUtils.putValues(types, values, buffer);
    return buffer;
  }

  private static int calculateLength(List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException {
    int res = 0;
    for (int i = 0; i < types.size(); i++) {
      // types
      res += Byte.BYTES;
      switch (types.get(i)) {
        case BOOLEAN:
          res += 1;
          break;
        case INT32:
          res += Integer.BYTES;
          break;
        case INT64:
          res += Long.BYTES;
          break;
        case FLOAT:
          res += Float.BYTES;
          break;
        case DOUBLE:
          res += Double.BYTES;
          break;
        case TEXT:
          res += Integer.BYTES;
          res += ((String) values.get(i)).getBytes(TSFileConfig.STRING_CHARSET).length;
          break;
        default:
          throw new IoTDBConnectionException(MSG_UNSUPPORTED_DATA_TYPE + types.get(i));
      }
    }
    return res;
  }

  /**
   * put value in buffer
   *
   * @param types types list
   * @param values values list
   * @param buffer buffer to insert
   * @throws IoTDBConnectionException
   */
  private static void putValues(List<TSDataType> types, List<Object> values, ByteBuffer buffer)
      throws IoTDBConnectionException {
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        ReadWriteIOUtils.write(TYPE_NULL, buffer);
        continue;
      }
      ReadWriteIOUtils.write(types.get(i), buffer);
      switch (types.get(i)) {
        case BOOLEAN:
          ReadWriteIOUtils.write((Boolean) values.get(i), buffer);
          break;
        case INT32:
          ReadWriteIOUtils.write((Integer) values.get(i), buffer);
          break;
        case INT64:
          ReadWriteIOUtils.write((Long) values.get(i), buffer);
          break;
        case FLOAT:
          ReadWriteIOUtils.write((Float) values.get(i), buffer);
          break;
        case DOUBLE:
          ReadWriteIOUtils.write((Double) values.get(i), buffer);
          break;
        case TEXT:
          byte[] bytes = ((String) values.get(i)).getBytes(TSFileConfig.STRING_CHARSET);
          ReadWriteIOUtils.write(bytes.length, buffer);
          buffer.put(bytes);
          break;
        default:
          throw new IoTDBConnectionException(MSG_UNSUPPORTED_DATA_TYPE + types.get(i));
      }
    }
    buffer.flip();
  }

  private static void getValueBufferOfDataType(
      TSDataType dataType, Tablet tablet, int i, ByteBuffer valueBuffer) {

    switch (dataType) {
      case INT32:
        int[] intValues = (int[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (tablet.bitMaps == null
              || tablet.bitMaps[i] == null
              || !tablet.bitMaps[i].isMarked(index)) {
            valueBuffer.putInt(intValues[index]);
          } else {
            valueBuffer.putInt(Integer.MIN_VALUE);
          }
        }
        break;
      case INT64:
        long[] longValues = (long[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (tablet.bitMaps == null
              || tablet.bitMaps[i] == null
              || !tablet.bitMaps[i].isMarked(index)) {
            valueBuffer.putLong(longValues[index]);
          } else {
            valueBuffer.putLong(Long.MIN_VALUE);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (tablet.bitMaps == null
              || tablet.bitMaps[i] == null
              || !tablet.bitMaps[i].isMarked(index)) {
            valueBuffer.putFloat(floatValues[index]);
          } else {
            valueBuffer.putFloat(Float.MIN_VALUE);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (tablet.bitMaps == null
              || tablet.bitMaps[i] == null
              || !tablet.bitMaps[i].isMarked(index)) {
            valueBuffer.putDouble(doubleValues[index]);
          } else {
            valueBuffer.putDouble(Double.MIN_VALUE);
          }
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          if (tablet.bitMaps == null
              || tablet.bitMaps[i] == null
              || !tablet.bitMaps[i].isMarked(index)) {
            valueBuffer.put(BytesUtils.boolToByte(boolValues[index]));
          } else {
            valueBuffer.put(BytesUtils.boolToByte(false));
          }
        }
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) tablet.values[i];
        for (int index = 0; index < tablet.rowSize; index++) {
          valueBuffer.putInt(binaryValues[index].getLength());
          valueBuffer.put(binaryValues[index].getValues());
        }
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported.", dataType));
    }
  }

  public static List<EndPoint> parseSeedNodeUrls(List<String> nodeUrls) {
    if (nodeUrls == null) {
      throw new NumberFormatException("nodeUrls is null");
    }
    List<EndPoint> endPointsList = new ArrayList<>();
    for (String nodeUrl : nodeUrls) {
      EndPoint endPoint = parseNodeUrl(nodeUrl);
      endPointsList.add(endPoint);
    }
    return endPointsList;
  }

  private static EndPoint parseNodeUrl(String nodeUrl) {
    EndPoint endPoint = new EndPoint();
    String[] split = nodeUrl.split(":");
    if (split.length != 2) {
      throw new NumberFormatException("NodeUrl Incorrect format");
    }
    String ip = split[0];
    try {
      int rpcPort = Integer.parseInt(split[1]);
      return endPoint.setIp(ip).setPort(rpcPort);
    } catch (Exception e) {
      throw new NumberFormatException("NodeUrl Incorrect format");
    }
  }

  /**
   * Generate by class to Template
   *
   * @param aclass
   * @return
   * @throws StatementExecutionException
   */
  public static Template classToTemplate(Class<?> aclass) throws StatementExecutionException {
    Field[] declaredFields = aclass.getDeclaredFields();
    Template template = new Template(aclass.getSimpleName());
    for (Field f : declaredFields) {
      final String name = f.getType().getName();
      switch (name) {
        case "java.lang.String":
          template.addToTemplate(
              new MeasurementNode(
                  f.getName(), TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY));
          break;
        case "java.lang.Boolean":
          template.addToTemplate(
              new MeasurementNode(
                  f.getName(), TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY));
          break;
        case "java.lang.Integer":
          template.addToTemplate(
              new MeasurementNode(
                  f.getName(), TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY));
          break;
        case "java.lang.Long":
          template.addToTemplate(
              new MeasurementNode(
                  f.getName(), TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY));
          break;
        case "java.lang.Float":
          template.addToTemplate(
              new MeasurementNode(
                  f.getName(), TSDataType.FLOAT, TSEncoding.PLAIN, CompressionType.SNAPPY));
          break;
        case "java.lang.Double":
          template.addToTemplate(
              new MeasurementNode(
                  f.getName(), TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY));
          break;
        case "java.util.Date":
          template.addToTemplate(
              new MeasurementNode(
                  f.getName(), TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY));
          break;
        default:
          template.addToTemplate(
              new MeasurementNode(
                  f.getName(), TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY));
      }
    }
    return template;
  }

  /**
   * Add a record entry parameter based on entity generation
   *
   * @param object
   * @return
   */
  public static InsertRecordParam objectToInsertRecordParam(Object object) {
    Field[] declaredFields = object.getClass().getDeclaredFields();
    List<String> subMeasurementsList = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    List<Object> valueList = new ArrayList<>();
    for (Field f : declaredFields) {
      f.setAccessible(true);
      try {
        final String typeName = f.getType().getName();
        final String name = f.getName();
        final Object o1 = f.get(object);
        if (o1 != null) {
          TSDataType tsDataType;
          switch (typeName) {
            case "java.lang.String":
              tsDataType = TSDataType.TEXT;
              subMeasurementsList.add(name);
              tsDataTypes.add(tsDataType);
              valueList.add(o1);
              break;
            case "java.lang.Boolean":
              tsDataType = TSDataType.BOOLEAN;
              subMeasurementsList.add(name);
              tsDataTypes.add(tsDataType);
              valueList.add(o1);
              break;
            case "java.lang.Integer":
              tsDataType = TSDataType.INT32;
              subMeasurementsList.add(name);
              tsDataTypes.add(tsDataType);
              valueList.add(o1);
              break;
            case "java.lang.Long":
              tsDataType = TSDataType.INT64;
              subMeasurementsList.add(name);
              tsDataTypes.add(tsDataType);
              valueList.add(o1);
              break;
            case "java.lang.Float":
              tsDataType = TSDataType.FLOAT;
              subMeasurementsList.add(name);
              tsDataTypes.add(tsDataType);
              valueList.add(o1);
              break;
            case "java.lang.Double":
              tsDataType = TSDataType.DOUBLE;
              subMeasurementsList.add(name);
              tsDataTypes.add(tsDataType);
              valueList.add(o1);
              break;
            case "java.util.Date":
              tsDataType = TSDataType.INT64;
              subMeasurementsList.add(name);
              tsDataTypes.add(tsDataType);
              valueList.add(((Date) o1).getTime());
              break;
            default:
              tsDataType = TSDataType.TEXT;
              subMeasurementsList.add(name);
              tsDataTypes.add(tsDataType);
              valueList.add(o1);
          }
        }
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    return new InsertRecordParam(subMeasurementsList, tsDataTypes, valueList);
  }

  public static class InsertRecordParam {
    List<String> subMeasurementsList;
    List<TSDataType> tsDataTypes;
    List<Object> valueList;

    public InsertRecordParam(
        List<String> subMeasurementsList, List<TSDataType> tsDataTypes, List<Object> valueList) {
      this.subMeasurementsList = subMeasurementsList;
      this.tsDataTypes = tsDataTypes;
      this.valueList = valueList;
    }

    public List<String> getSubMeasurementsList() {
      return subMeasurementsList;
    }

    public void setSubMeasurementsList(List<String> subMeasurementsList) {
      this.subMeasurementsList = subMeasurementsList;
    }

    public List<TSDataType> getTsDataTypes() {
      return tsDataTypes;
    }

    public void setTsDataTypes(List<TSDataType> tsDataTypes) {
      this.tsDataTypes = tsDataTypes;
    }

    public List<Object> getValueList() {
      return valueList;
    }

    public void setValueList(List<Object> valueList) {
      this.valueList = valueList;
    }
  }

  /**
   * Any level of splicing entity simple name generation prefixPath
   *
   * @param aclass
   * @param prefixs
   * @return
   */
  public static String prefixPath(Class<?> aclass, String... prefixs) {
    final String reduce =
        Arrays.stream(prefixs)
            .reduce("", (current, str) -> "".equals(current) ? str : current + "." + str);
    return reduce == null || "".equals(reduce)
        ? aclass.getSimpleName()
        : reduce.concat("." + aclass.getSimpleName());
  }

  /**
   * Any level of splicing entity attribute name generation prefixPaths
   *
   * @param aclass
   * @param prefixs
   * @return
   */
  public static List<String> classToPaths(Class<?> aclass, String... prefixs) {
    final String prefixPath = prefixPath(aclass, prefixs);
    List<String> paths = new ArrayList<String>();
    Arrays.stream(aclass.getDeclaredFields())
        .map(Field::getName)
        .forEach(
            name -> {
              paths.add(prefixPath + "." + name);
            });
    return paths;
  }

  /**
   * Query arbitrary level data based on time and return Java array
   *
   * @param session
   * @param aclass
   * @param startTime
   * @param endTime
   * @param prefixs
   * @param <T>
   * @return
   * @throws IoTDBConnectionException
   * @throws StatementExecutionException
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws NoSuchFieldException
   */
  public static <T> List<T> executeRawDataQuery(
      Session session, Class<T> aclass, long startTime, long endTime, String... prefixs)
      throws IoTDBConnectionException, StatementExecutionException, InstantiationException,
          IllegalAccessException, NoSuchFieldException {
    List<String> paths = SessionUtils.classToPaths(aclass, prefixs);
    final List<T> data = new ArrayList<>();
    SessionDataSet sessionDataSet = session.executeRawDataQuery(paths, startTime, endTime);
    ;
    while (sessionDataSet.hasNext()) {
      final List<org.apache.iotdb.tsfile.read.common.Field> fields =
          sessionDataSet.next().getFields();
      if (fields.size() == paths.size()) {
        final T newData = aclass.newInstance();
        for (int i = 0; i < paths.size(); i++) {
          final String[] split = paths.get(i).split("\\.");
          final String s = split[split.length - 1];
          Field f = aclass.getDeclaredField(s);
          f.setAccessible(true);
          final String typeName = f.getType().getName();
          if (fields.get(i).getDataType() != null) {
            switch (typeName) {
              case "java.lang.String":
                f.set(newData, fields.get(i).getStringValue());
                break;
              case "java.lang.Boolean":
                f.set(newData, fields.get(i).getBoolV());
                break;
              case "java.lang.Integer":
                f.set(newData, fields.get(i).getIntV());
                break;
              case "java.lang.Long":
                f.set(newData, fields.get(i).getLongV());
                break;
              case "java.lang.Float":
                f.set(newData, fields.get(i).getFloatV());
                break;
              case "java.lang.Double":
                f.set(newData, fields.get(i).getDoubleV());
                break;
              case "java.util.Date":
                final long l = Long.parseLong(fields.get(i).toString());
                final Date date = new Date();
                date.setTime(l);
                f.set(newData, date);
                break;
              default:
                f.set(newData, String.valueOf(fields.get(i)));
            }
          }
        }
        data.add(newData);
      }
    }
    sessionDataSet.closeOperationHandle();
    return data;
  }
}
