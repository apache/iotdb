/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.metadata.mnode.*;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;

/**
 * This class translate an IMNode into a bytebuffer, or otherwise. Expected to support record as
 * entry of segment-level index further. Coupling with IMNode structure.
 */
public class RecordUtils {

  /**
   * Entry of the Class
   *
   * @param node
   * @return
   */
  public static ByteBuffer node2Buffer(IMNode node) {
    if (node.isMeasurement()) {
      return measurement2Buffer(node.getAsMeasurementMNode());
    } else {
      return internal2Buffer(node);
    }
  }

  /**
   * Internal/Entity MNode Record Structure (in bytes):
   * <br>(fixed length record)
   * <ul>
   * <li>1 byte: nodeType, 0 for internal, 1 for entity, 4 for measurement
   * <li>1 short (2 bytes): recLen, length of record (remove it may reduce space overhead while a bit slower)
   * <li>1 long (8 bytes): glbIndex, combined index to its children records
   * <li>1 byte: templateIndex, index for template
   * </ul>
   * -- bitwise flags --
   * <ul>
   * <li>1 bit: usingTemplate, whether using template 1 bit: isAligned
   * </ul>
   * @param node
   * @return
   */
  private static ByteBuffer internal2Buffer(IMNode node) {
    byte nodeType = 0;
    boolean isAligned = false;
    short recLen = (short) 1 + 2 + 8 + 1 + 1;

    if (node.isEntity()) {
      nodeType = 1;
      isAligned = node.getAsEntityMNode().isAligned();
    }

    ByteBuffer buffer = ByteBuffer.allocate(recLen);
    ReadWriteIOUtils.write(nodeType, buffer);
    ReadWriteIOUtils.write(recLen, buffer);
    ReadWriteIOUtils.write(
        ICachedMNodeContainer.getCachedMNodeContainer(node).getSegmentAddress(), buffer);
    ReadWriteIOUtils.write(convertTemplate2Byte(node.getSchemaTemplate()), buffer);

    // encode bitwise flag
    byte useAndAligned = encodeInternalStatus(node.isUseTemplate(), isAligned);

    ReadWriteIOUtils.write(useAndAligned, buffer);
    return buffer;
  }

  /**
   * TODO: properties and tags unhandled yet
   * <p>Measurement MNode Record Structure:
   * <br>(var length record, with length member)
   * <ul>
   * <li>1 byte: nodeType, as above
   * <li>1 short (2 bytes): recLength, length of whole record
   * <li>1 long (8 bytes): tagIndex, related to tag module
   * <li>1 long (8 bytes): schemaBytes, including datatype/compressionType/encoding and so on
   * <li>var length string (4+var_length bytes): alias
   * </ul>
   * <p>It doesn't use MeasurementSchema.serializeTo for duplication of measurementId
   */
  private static ByteBuffer measurement2Buffer(IMeasurementMNode node) {
    byte nodeType = 4;

    int bufferLength = 1 + 2 + 8 + 8 + (node.getAlias().getBytes().length + 4);
    ByteBuffer buffer = ByteBuffer.allocate(bufferLength);

    ReadWriteIOUtils.write(nodeType, buffer);
    ReadWriteIOUtils.write((short) bufferLength, buffer);
    ReadWriteIOUtils.write(convertTags2Long(node), buffer);
    ReadWriteIOUtils.write(convertSchema2Long(node), buffer);
    ReadWriteIOUtils.write(node.getAlias(), buffer);
    return buffer;
  }

  /**
   * NOTICE: Make sure that buffer has set its position and limit clearly before pass to this
   * method.
   *
   * @param nodeName name of the constructed node
   * @param buffer content of the node
   * @return node constructed from buffer
   */
  public static IMNode buffer2Node(String nodeName, ByteBuffer buffer) {
    IMNode resNode;

    byte nodeType = ReadWriteIOUtils.readByte(buffer);
    if (nodeType < 2) {
      // internal or entity node

      short recLen = ReadWriteIOUtils.readShort(buffer);
      long segAddr = ReadWriteIOUtils.readLong(buffer);
      byte templateIndex = ReadWriteIOUtils.readByte(buffer);
      byte bitFlag = ReadWriteIOUtils.readByte(buffer);

      boolean usingTemplate = usingTemplate(bitFlag);
      boolean isAligned = isAligned(bitFlag);

      if (nodeType == 0) {
        resNode = new InternalMNode(null, nodeName);
      } else {
        resNode = new EntityMNode(null, nodeName);
        resNode.getAsEntityMNode().setAligned(isAligned);
      }

      ICachedMNodeContainer.getCachedMNodeContainer(resNode).setSegmentAddress(segAddr);
      resNode.setUseTemplate(usingTemplate);

      return paddingTemplate(resNode, templateIndex);
    } else {
      // measurement node
      short recLenth = ReadWriteIOUtils.readShort(buffer);
      long tagIndex = ReadWriteIOUtils.readLong(buffer);
      long schemaByte = ReadWriteIOUtils.readLong(buffer);
      String alias = ReadWriteIOUtils.readString(buffer);

      return paddingMeasurement(nodeName, tagIndex, schemaByte, alias);
    }
  }

  public static short getRecordLength(ByteBuffer recBuf) {
    int oriPos = recBuf.position();
    recBuf.position(oriPos + 1);
    short len = ReadWriteIOUtils.readShort(recBuf);
    recBuf.position(oriPos);
    return len;
  }

  public static byte getRecordType(ByteBuffer recBuf) {
    int oriPos = recBuf.position();
    recBuf.position(oriPos);
    byte type = ReadWriteIOUtils.readByte(recBuf);
    recBuf.position(oriPos);
    return type;
  }

  public static long getRecordSegAddr(ByteBuffer recBuf) {
    int oriPos = recBuf.position();
    recBuf.position(oriPos + 3);
    long addr = ReadWriteIOUtils.readLong(recBuf);
    recBuf.position(oriPos);
    return addr;
  }

  public static String getRecordAlias(ByteBuffer recBuf) {
    int oriPos = recBuf.position();
    recBuf.position(oriPos + 19);
    String alias = ReadWriteIOUtils.readString(recBuf);
    recBuf.position(oriPos);
    return alias;
  }

  public static void updateSegAddr(ByteBuffer recBuf, long newSegAddr) {
    int oriPos = recBuf.position();
    recBuf.position(oriPos + 3);
    ReadWriteIOUtils.write(newSegAddr, recBuf);
    recBuf.position(oriPos);
  }

  @TestOnly
  public static String buffer2String(ByteBuffer buffer) {
    StringBuilder builder = new StringBuilder("[");
    IMNode node = buffer2Node("unspecified", buffer);
    if (node.isMeasurement()) {
      builder.append("measurementNode, ");
      builder.append(String.format("alias: %s, ", node.getAsMeasurementMNode().getAlias()));
      builder.append(
          String.format("type: %s, ", node.getAsMeasurementMNode().getDataType("").toString()));
      builder.append(
          String.format(
              "encoding: %s, ",
              node.getAsMeasurementMNode().getSchema().getEncodingType().toString()));
      builder.append(
          String.format(
              "compressor: %s]",
              node.getAsMeasurementMNode().getSchema().getCompressor().toString()));
      return builder.toString();
    } else if (node.isEntity()) {
      builder.append("entityNode, ");

      if (node.getAsEntityMNode().isAligned()) {
        builder.append("aligned, ");
      } else {
        builder.append("not aligned, ");
      }

    } else {
      builder.append("internalNode, ");
    }

    if (node.isUseTemplate()) {
      builder.append("using template.]");
    } else {
      builder.append("not using template.]");
    }

    return builder.toString();
  }

  // region padding with IMNode
  /** These 2 convert methods are coupling with tag, template module respectively. */
  private static long convertTags2Long(IMeasurementMNode node) {
    return 0L;
  }

  private static byte convertTemplate2Byte(Template temp) {
    return 0;
  }

  private static IMNode paddingTemplate(IMNode node, byte templateIndex) {
    return node;
  }

  private static long convertSchema2Long(IMeasurementMNode node) {
    byte dataType = node.getSchema().getTypeInByte();
    byte encoding = node.getSchema().getEncodingType().serialize();
    byte compressor = node.getSchema().getCompressor().serialize();

    return (dataType << 16 | encoding << 8 | compressor);
  }

  private static IMNode paddingMeasurement(
      String nodeName, long tagIndex, long schemaBytes, String alias) {
    byte dataType = (byte) (schemaBytes >>> 16);
    byte encoding = (byte) ((schemaBytes >>> 8) & 0xffL);
    byte compressor = (byte) (schemaBytes & 0xffL);

    IMeasurementSchema schema =
        new MeasurementSchema(
            nodeName,
            TSDataType.values()[dataType],
            TSEncoding.values()[encoding],
            CompressionType.values()[compressor]);

    return MeasurementMNode.getMeasurementMNode(null, nodeName, schema, alias);
  }

  // endregion

  // region codex for bit flag

  private static byte encodeInternalStatus(boolean usingTemplate, boolean isAligned) {
    byte flag = 0;
    if (usingTemplate) {
      flag |= 0x01;
    }
    if (isAligned) {
      flag |= 0x02;
    }
    return flag;
  }

  private static boolean isAligned(byte flag) {
    return (flag & 0x02) == 2;
  }

  private static boolean usingTemplate(byte flag) {
    return (flag & 0x01) == 1;
  }

  // endregion
}
