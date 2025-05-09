package org.apache.iotdb.session.compress;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RpcEncoder {
  private final MetaHead metaHead;
  private final Map<TSDataType, TSEncoding> columnEncodersMap;
  private final CompressionType compressionType;
  private List<byte[]> encodedData;

  public RpcEncoder(
      Map<TSDataType, TSEncoding> columnEncodersMap, CompressionType compressionType) {
    this.columnEncodersMap = columnEncodersMap;
    this.compressionType = compressionType;
    this.metaHead = new MetaHead();
    this.encodedData = new ArrayList<>();
  }

  /**
   * 获取 tablet 中的时间戳列，然后编码放入 encodedData1 中
   *
   * @param tablet 数据
   * @throws IOException 如果编码过程中发生IO错误
   */
  public ByteBuffer encodeTimestamps(Tablet tablet) {
    TSEncoding encoding = columnEncodersMap.getOrDefault(TSDataType.INT64, TSEncoding.PLAIN);
    ColumnEncoder encoder = createEncoder(TSDataType.INT64, encoding);

    // 1.获取时间戳数据
    long[] timestamps = tablet.getTimestamps();
    List<Long> timestampsList = new ArrayList<>();
    // 2.转 List
    for (int i = 0; i < timestamps.length; i++) {
      timestampsList.add(timestamps[i]);
    }
    // 3.编码
    byte[] encoded = encoder.encode(timestampsList);
    ByteBuffer timeBuffer = ByteBuffer.wrap(encoded);
    // 4.调整 ByteBuffer 的 limit 为实际写入的数据长度
    timeBuffer.flip();
    return timeBuffer;
  }

  /** 获取 tablet 中的值列，然后编码放入 encodedData2 中 TODO 还有很多考虑 */
  public ByteBuffer encodeValues(Tablet tablet) {
    // 1. 预估最大空间（假设每列最大为 rowSize * 16 字节）
    int estimatedSize = tablet.getRowSize() * 16 * tablet.getSchemas().size();
    ByteBuffer valueBuffer = ByteBuffer.allocate(estimatedSize);

    // 2. 编码每一列
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      IMeasurementSchema schema = tablet.getSchemas().get(i);
      byte[] encoded = encodeColumn(schema.getType(), tablet, i);
      valueBuffer.put(encoded);
    }

    // 3. 序列化
    byte[] metaHeadEncoder = getMetaHead().toBytes();
    valueBuffer.put(metaHeadEncoder);
    // 4. metaHead 长度
    valueBuffer.putInt(metaHeadEncoder.length);
    // 5. 调整 ByteBuffer 的 limit 为实际写入的数据长度
    valueBuffer.flip();
    return valueBuffer;
  }

  public byte[] encodeColumn(TSDataType dataType, Tablet tablet, int columnIndex) {
    // 1.获取编码类型
    TSEncoding encoding = columnEncodersMap.getOrDefault(dataType, TSEncoding.PLAIN);
    ColumnEncoder encoder = createEncoder(dataType, encoding);
    // 2.获取该列的数据并转换为 List
    Object columnValues = tablet.getValues()[columnIndex];
    List<?> valueList;
    switch (dataType) {
      case INT32:
        int[] intArray = (int[]) columnValues;
        List<Integer> intList = new ArrayList<>(intArray.length);
        for (int v : intArray) intList.add(v);
        valueList = intList;
        break;
      case INT64:
        long[] longArray = (long[]) columnValues;
        List<Long> longList = new ArrayList<>(longArray.length);
        for (long v : longArray) longList.add(v);
        valueList = longList;
        break;
      case FLOAT:
        float[] floatArray = (float[]) columnValues;
        List<Float> floatList = new ArrayList<>(floatArray.length);
        for (float v : floatArray) floatList.add(v);
        valueList = floatList;
        break;
      case DOUBLE:
        double[] doubleArray = (double[]) columnValues;
        List<Double> doubleList = new ArrayList<>(doubleArray.length);
        for (double v : doubleArray) doubleList.add(v);
        valueList = doubleList;
        break;
      case BOOLEAN:
        boolean[] boolArray = (boolean[]) columnValues;
        List<Boolean> boolList = new ArrayList<>(boolArray.length);
        for (boolean v : boolArray) boolList.add(v);
        valueList = boolList;
        break;
      case TEXT:
        Object[] textArray = (Object[]) columnValues;
        List<Object> textList = new ArrayList<>(textArray.length);
        for (Object v : textArray) textList.add(v);
        valueList = textList;
        break;
      default:
        throw new UnsupportedOperationException("不支持的数据类型: " + dataType);
    }

    // 3.编码
    byte[] encoded = encoder.encode(valueList);
    // 4.获取 ColumnEntry 内容并添加到 metaHead
    metaHead.addColumnEntry(encoder.getColumnEntry());
    // 5.将编码后的数据添加到 encodedData
    encodedData.add(encoded);
    // 6.返回编码后的数据
    return encoded;
  }

  /**
   * 根据数据类型和编码类型创建对应的编码器
   *
   * @param dataType 数据类型
   * @param encodingType 编码类型
   * @return 列编码器
   */
  private ColumnEncoder createEncoder(TSDataType dataType, TSEncoding encodingType) {
    switch (encodingType) {
      case PLAIN:
        return new PlainColumnEncoder(dataType);
      case RLE:
        return new RleColumnEncoder(dataType);
      case TS_2DIFF:
        return new Ts2DiffColumnEncoder(dataType);
      default:
        throw new EncodingTypeNotSupportedException(encodingType.name());
    }
  }

  /**
   * 获取编码后的数据
   *
   * @return 编码后的数据列表
   */
  public List<byte[]> getEncodedData() {
    return encodedData;
  }

  /**
   * 获取元数据头
   *
   * @return 元数据头
   */
  public MetaHead getMetaHead() {
    return metaHead;
  }
}
