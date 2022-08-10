package org.apache.iotdb.tool.core.service;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;

public class TsFileReader {

  public static void main(String[] args) throws Exception {
    File tsFile = new File("/Users/liupengfei_1/Downloads/tsfile/1650952199003-7-0-0.tsfile");
    scanIndex(tsFile);
    //        scanDatas(tsFile);
  }

  /**
   * 遍历数据 正向扫描
   *
   * @throws Exception
   */
  private static void scanDatas(File tsFile) throws Exception {
    FileInputStream fis = new FileInputStream(tsFile);
    // 比较魔数
    byte[] magic = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
    byte[] magicCur = new byte[magic.length];
    fis.read(magicCur);
    System.out.println();
    byte[] versionCur = new byte[1];
    fis.read(versionCur);
    System.out.println(
        "魔数比对："
            + Arrays.equals(magic, magicCur)
            + "，版本比对："
            + (TSFileConfig.VERSION_NUMBER == versionCur[0]));
    int metaMarker = 0;
    int chunkNum = 0;
    while ((metaMarker = fis.read()) != -1) {
      if (metaMarker == MetaMarker.CHUNK_GROUP_HEADER) {
        // -----------------------读取块组-----------------------
        readGroupChunk(fis);
        chunkNum = 0;
      } else if (metaMarker == MetaMarker.CHUNK_HEADER
          || metaMarker == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        // -----------------------读取块信息----------------------
        System.out.println("----第" + (++chunkNum) + "块");
        // 块头信息
        int measurementIdLen = ReadWriteForEncodingUtils.readVarInt(fis);
        byte[] measurementId = null;
        if (measurementIdLen != -1) {
          measurementId = new byte[measurementIdLen];
          fis.read(measurementId);
        }
        int dataSize = ReadWriteForEncodingUtils.readUnsignedVarInt(fis);
        byte[] dataTypeBs = new byte[1];
        fis.read(dataTypeBs);
        TSDataType tsDataType = TSDataType.deserialize(dataTypeBs[0]);
        byte[] compressionTypeBs = new byte[1];
        fis.read(compressionTypeBs);
        CompressionType compressionType = CompressionType.deserialize(compressionTypeBs[0]);
        byte[] encodingTypeBs = new byte[1];
        fis.read(encodingTypeBs);
        TSEncoding tsEncoding = TSEncoding.deserialize(encodingTypeBs[0]);
        System.out.println(
            "----"
                + "measurmentId:"
                + ((measurementIdLen == -1)
                    ? null
                    : new String(measurementId, TSFileConfig.STRING_CHARSET))
                + ""
                + ",dataSize:"
                + dataSize
                + ""
                + ",dataType:"
                + tsDataType
                + ",compression:"
                + compressionType.name()
                + ","
                + "encoding:"
                + tsEncoding.name());
        byte[] data = new byte[dataSize];
        fis.read(data);
        // 页信息
        ByteBuffer dataBuffer = ByteBuffer.wrap(data);
        int pageNum = 1;
        while (dataBuffer.remaining() > 0) {
          System.out.println("--读取第" + pageNum + "页");
          // 读取页信息
          readPage(
              dataBuffer,
              tsDataType,
              compressionType,
              tsEncoding,
              metaMarker == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER,
              new String(measurementId, TSFileConfig.STRING_CHARSET));
          pageNum++;
        }
        dataBuffer.clear();
        dataBuffer = null;
      } else if (metaMarker == MetaMarker.OPERATION_INDEX_RANGE) {
        // -----------------------读取index范围，还不知道index是啥，好像是集群raft相关的----------------------
        System.out.println(
            "------------------------------------数据部分已经完成-------------------------------------------------");
        System.out.println("------------读取index范围-----------");
        long minPlanIndex = ReadWriteIOUtils.readLong(fis);
        long maxPlanIndex = ReadWriteIOUtils.readLong(fis);
        System.out.println("----minPlanIndex:" + minPlanIndex + ",maxPlanIndex:" + maxPlanIndex);
      } else if (metaMarker == MetaMarker.SEPARATOR) {
        System.out.println("------------已经扫描到索引部分，数据扫描完成-----------");
        break;
      } else {
        System.out.println("暂时不知道是啥:" + metaMarker);
        break;
      }
    }

    fis.close();
  }

  /**
   * 扫描索引 倒着读
   *
   * @param tsFile
   * @throws Exception
   */
  private static void scanIndex(File tsFile) throws Exception {

    byte[] magic = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
    FileInputStream fis = new FileInputStream(tsFile);
    // 分配魔数+size长度
    ByteBuffer msBuffer = ByteBuffer.allocate(magic.length + ReadWriteIOUtils.INT_LEN);
    FileChannel channel = fis.getChannel();
    channel.read(msBuffer, channel.size() - msBuffer.capacity());
    // 比较魔数
    msBuffer.position(msBuffer.capacity() - magic.length);
    for (int i = 0; i < magic.length; i++) {
      if (magic[i] != msBuffer.get()) {
        throw new Exception("footer magic check fail");
      }
    }
    System.out.println("魔数比较成功");
    // 读取size
    int curLimit = msBuffer.capacity() - magic.length;
    int curPosition = curLimit - ReadWriteIOUtils.INT_LEN;
    msBuffer.limit(curLimit);
    msBuffer.position(curPosition);
    int size = ReadWriteIOUtils.readInt(msBuffer);
    System.out.println("size:" + size);
    msBuffer.clear();
    msBuffer = null;
    // 读取索引root
    ByteBuffer rootIndexBuffer = ByteBuffer.allocate(size);
    channel.read(rootIndexBuffer, channel.size() - size - magic.length - ReadWriteIOUtils.INT_LEN);
    rootIndexBuffer.position(0);
    TsFileMetadata tsFileMetadata = TsFileMetadata.deserializeFrom(rootIndexBuffer);
    rootIndexBuffer.clear();
    rootIndexBuffer = null;
    System.out.println("metaOffset:" + tsFileMetadata.getMetaOffset());
    if (tsFileMetadata != null) {
      int level = 0;
      MetadataIndexNode indexRoot = tsFileMetadata.getMetadataIndex();
      scanIndexNode(indexRoot, channel, level + 1);
    }
    fis.close();
  }

  /** 扫描索引节点 */
  private static void scanIndexNode(MetadataIndexNode node, FileChannel channel, int level)
      throws Exception {

    if (node == null) {
      System.out.println("层:" + level + "为空");
      return;
    }
    System.out.println(
        "层:" + level + ",节点类型:" + node.getNodeType() + ",endOffset:" + node.getEndOffset());
    List<MetadataIndexEntry> childrens = node.getChildren();
    if (childrens == null || childrens.size() == 0) {
      System.out.println("层:" + level + "child为空");
      return;
    }
    boolean isLeafMeasurment = (node.getNodeType() == MetadataIndexNodeType.LEAF_MEASUREMENT);
    for (int i = 0; i < childrens.size(); i++) {
      MetadataIndexEntry curEntry = childrens.get(i);
      System.out.println("entry:" + curEntry.getName() + ",offset:" + curEntry.getOffset());
      long start = curEntry.getOffset();
      long end = node.getEndOffset();
      if (i != childrens.size() - 1) {
        end = childrens.get(i + 1).getOffset();
      }
      ByteBuffer buffer = ByteBuffer.allocate((int) (end - start));
      channel.read(buffer, start);
      buffer.position(0);
      while (buffer.remaining() > 0) {
        if (isLeafMeasurment) {
          buffer.mark();
          byte timeSeriesMetadataType = buffer.get();
          boolean needChunkMetadata = (timeSeriesMetadataType == 1);
          buffer.reset();
          TimeseriesMetadata timeseriesMetadata =
              TimeseriesMetadata.deserializeFrom(buffer, needChunkMetadata);
          System.out.println("timeseriesMetadata:" + timeseriesMetadata.toString());
        } else {
          MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
          scanIndexNode(metadataIndexNode, channel, level + 1);
        }
      }
    }
  }

  /**
   * 读取group chunk
   *
   * @param fis
   * @throws IOException
   */
  private static void readGroupChunk(FileInputStream fis) throws IOException {
    // 块组头信息
    int deviceIdLen = ReadWriteForEncodingUtils.readVarInt(fis);
    byte[] deviceId = new byte[deviceIdLen];
    fis.read(deviceId);
    String deviceIdStr = new String(deviceId, TSFileConfig.STRING_CHARSET);
    System.out.println("------设备id（length）:" + deviceIdStr + "（" + deviceIdLen + "）");
  }

  /**
   * 读取页
   *
   * @param dataBuffer
   * @param tsDataType
   * @param compressionType
   * @param tsEncoding
   * @param isOnePage
   * @param measurementId
   * @throws Exception
   */
  private static void readPage(
      ByteBuffer dataBuffer,
      TSDataType tsDataType,
      CompressionType compressionType,
      TSEncoding tsEncoding,
      boolean isOnePage,
      String measurementId)
      throws Exception {
    int uncompressedSize = ReadWriteForEncodingUtils.readUnsignedVarInt(dataBuffer);
    int compressedSize = ReadWriteForEncodingUtils.readUnsignedVarInt(dataBuffer);
    Statistics<? extends Serializable> statistics = null;
    if (!isOnePage) {
      statistics = Statistics.deserialize(dataBuffer, tsDataType);
    }
    System.out.println(
        "--页头信息，uncompressedSize:"
            + uncompressedSize
            + ",compressedSize:"
            + compressedSize
            + ",statistics:"
            + statistics);
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(compressionType);
    byte[] uncompressedBytes = new byte[uncompressedSize];
    byte[] compressedBytes = new byte[compressedSize];
    dataBuffer.get(compressedBytes);
    unCompressor.uncompress(compressedBytes, 0, compressedSize, uncompressedBytes, 0);
    ByteBuffer tempPageBuffer = ByteBuffer.wrap(uncompressedBytes);
    int timeOutSize = ReadWriteForEncodingUtils.readUnsignedVarInt(tempPageBuffer);
    Decoder timeDecoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType());
    Decoder valueDecoder = Decoder.getDecoderByType(tsEncoding, tsDataType);
    System.out.print("--times:");
    int timeEndPosition = tempPageBuffer.position() + timeOutSize;
    int timeCount = 0;
    tempPageBuffer.limit(timeEndPosition);
    while (timeDecoder.hasNext(tempPageBuffer)) {
      System.out.print(timeDecoder.readLong(tempPageBuffer));
      System.out.print(",");
      timeCount++;
    }
    tempPageBuffer.limit(tempPageBuffer.capacity());
    System.out.println("");
    System.out.println("--当前position:" + tempPageBuffer.position() + "，timeOutSize:" + timeOutSize);
    if (tempPageBuffer.position() > timeOutSize + 4) {
      throw new Exception("--timeOutSize读取错误");
    }
    System.out.print("--values:");
    int valueCount = 0;
    while (valueDecoder.hasNext(tempPageBuffer)) {
      valueCount++;
      switch (tsDataType) {
        case TEXT:
          valueDecoder.readBinary(tempPageBuffer);
          //                    System.out.print(valueDecoder.readBinary(tempPageBuffer));
          System.out.print(",");
          break;
        case FLOAT:
          valueDecoder.readFloat(tempPageBuffer);
          //                    System.out.print(valueDecoder.readFloat(tempPageBuffer));
          System.out.print(",");
          break;
        case INT32:
          //                        System.out.print(valueDecoder.readInt(tempPageBuffer));
          valueDecoder.readInt(tempPageBuffer);
          System.out.print(",");
          break;
        case INT64:
          //                    System.out.print(valueDecoder.readLong(tempPageBuffer));
          valueDecoder.readLong(tempPageBuffer);
          System.out.print(",");
          break;
        case DOUBLE:
          valueDecoder.readDouble(tempPageBuffer);
          //                    System.out.print(valueDecoder.readDouble(tempPageBuffer));
          System.out.print(",");
          break;
        case BOOLEAN:
          valueDecoder.readBoolean(tempPageBuffer);
          //                    System.out.print(valueDecoder.readBoolean(tempPageBuffer));
          System.out.print(",");
          break;
        case VECTOR:
          System.out.println("--暂时没看嗯");
          break;
        default:
          System.out.println("--不认识");
          break;
      }
    }
    if (timeCount != valueCount
        || (statistics == null ? false : statistics.getCount() != timeCount)) {
      throw new Exception("时间序列数量对不上");
    }
    System.out.println("");
    tempPageBuffer.clear();
    tempPageBuffer = null;
  }
}
