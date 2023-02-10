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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mtree.store.disk.ICachedMNodeContainer;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This class translate an IMNode into a bytebuffer, or vice versa. Expected to support record as
 * entry of segment-level index further. Coupling with IMNode structure.<br>
 * <br>
 * TODO: Guardian statements on higher stack NEEDED. The longest ALIAS is limited to 0x7fff(32767)
 * bytes for that a Short is used to record the length, hence a colossal record may collapse the
 * stack.
 */
public class RecordUtils {
  // Offsets of IMNode infos in a record buffer
  private static final short INTERNAL_NODE_LENGTH =
      (short) 1 + 2 + 8 + 4 + 1; // always fixed length record
  private static final short MEASUREMENT_BASIC_LENGTH =
      (short) 1 + 2 + 8 + 8; // final length depends on its alias and props

  /** These offset rather than magic number may also be used to track usage of related field. */
  private static final short LENGTH_OFFSET = 1;

  private static final short ALIAS_OFFSET = 19;
  private static final short SEG_ADDRESS_OFFSET = 3;
  private static final short SCHEMA_OFFSET = 11;
  private static final short INTERNAL_BITFLAG_OFFSET = 15;

  private static final byte INTERNAL_TYPE = 0;
  private static final byte ENTITY_TYPE = 1;
  private static final byte MEASUREMENT_TYPE = 4;

  public static ByteBuffer node2Buffer(IMNode node) {
    if (node.isMeasurement()) {
      return measurement2Buffer(node.getAsMeasurementMNode());
    } else {
      return internal2Buffer(node);
    }
  }

  /**
   * Internal/Entity MNode Record Structure (in bytes): <br>
   * (fixed length record)
   *
   * <ul>
   *   <li>1 byte: nodeType, 0 for internal, 1 for entity, 4 for measurement
   *   <li>1 short (2 bytes): recLen, length of record (remove it may reduce space overhead while a
   *       bit slower)
   *   <li>1 long (8 bytes): glbIndex, combined index to its children records
   *   <li>1 int (4 byte): templateId, id of template, occupies only 1 byte before
   * </ul>
   *
   * -- bitwise flags (1 byte) --
   *
   * <ul>
   *   <li>1 bit : usingTemplate, whether using template
   *   <li>1 bit : isAligned
   * </ul>
   *
   * @param node
   * @return
   */
  private static ByteBuffer internal2Buffer(IMNode node) {
    byte nodeType = INTERNAL_TYPE;
    boolean isAligned = false;

    if (node.isEntity()) {
      nodeType = ENTITY_TYPE;
      isAligned = node.getAsEntityMNode().isAligned();
    }

    ByteBuffer buffer = ByteBuffer.allocate(INTERNAL_NODE_LENGTH);
    ReadWriteIOUtils.write(nodeType, buffer);
    ReadWriteIOUtils.write(INTERNAL_NODE_LENGTH, buffer);
    ReadWriteIOUtils.write(
        ICachedMNodeContainer.getCachedMNodeContainer(node).getSegmentAddress(), buffer);
    ReadWriteIOUtils.write(node.getSchemaTemplateIdWithState(), buffer);

    // encode bitwise flag
    byte useAndAligned = encodeInternalStatus(node.isUseTemplate(), isAligned);

    ReadWriteIOUtils.write(useAndAligned, buffer);
    return buffer;
  }

  /**
   * It is convenient to expand the semantic of the statusBytes for further status of a measurement,
   * e.g., preDelete, since 8 bytes are far more sufficient to represent the schema of it.
   *
   * <p>Measurement MNode Record Structure: <br>
   * (var length record, with length member)
   *
   * <ul>
   *   <li>1 byte: nodeType, as above
   *   <li>1 short (2 bytes): recLength, length of whole record
   *   <li>1 long (8 bytes): tagIndex, value of the offset within a measurement
   *   <li>1 long (8 bytes): statusBytes, including datatype/compressionType/encoding and so on
   *   <li>var length string (4+var_length bytes): alias
   *   <li>var length map (4+var_length bytes): props, serialized by {@link ReadWriteIOUtils}
   * </ul>
   *
   * <p>It doesn't use MeasurementSchema.serializeTo for duplication of measurementId
   */
  private static ByteBuffer measurement2Buffer(IMeasurementMNode node) {
    int bufferLength =
        node.getAlias() == null
            ? 4 + MEASUREMENT_BASIC_LENGTH
            : (node.getAlias().getBytes().length + 4 + MEASUREMENT_BASIC_LENGTH);

    // consider props
    bufferLength += 4;
    if (node.getSchema().getProps() != null) {
      for (Map.Entry<String, String> e : node.getSchema().getProps().entrySet()) {
        bufferLength += 8 + e.getKey().getBytes().length + e.getValue().length();
      }
    }

    ByteBuffer buffer = ByteBuffer.allocate(bufferLength);

    ReadWriteIOUtils.write(MEASUREMENT_TYPE, buffer);
    ReadWriteIOUtils.write((short) bufferLength, buffer);
    ReadWriteIOUtils.write(convertTags2Long(node), buffer);
    ReadWriteIOUtils.write(convertMeasStat2Long(node), buffer);
    ReadWriteIOUtils.write(node.getAlias(), buffer);
    ReadWriteIOUtils.write(node.getSchema().getProps(), buffer);
    return buffer;
  }

  /**
   * NOTICE: Make sure that buffer has set its position and limit clearly before pass to this
   * method.<br>
   * <br>
   * FIXME: recLen is not futile although 'never used', since when decode from a buffer, this field
   * alleviate an extra mem read for length of alias. BUT it is indeed redundant with length of
   * alias which flushed by {@linkplain ReadWriteIOUtils#write(String, ByteBuffer)}, and error-prone
   * since it only contains 2 bytes while the latter contains 4.
   *
   * @param nodeName name of the constructed node
   * @param buffer content of the node
   * @return node constructed from buffer
   */
  public static IMNode buffer2Node(String nodeName, ByteBuffer buffer) throws MetadataException {
    IMNode resNode;

    byte nodeType = ReadWriteIOUtils.readByte(buffer);
    if (nodeType < 2) {
      // internal or entity node

      short recLen = ReadWriteIOUtils.readShort(buffer);
      long segAddr = ReadWriteIOUtils.readLong(buffer);
      int templateId = ReadWriteIOUtils.readInt(buffer);
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
      resNode.setSchemaTemplateId(templateId);

      return resNode;
    } else {
      // measurement node
      short recLenth = ReadWriteIOUtils.readShort(buffer);
      long tagIndex = ReadWriteIOUtils.readLong(buffer);
      long schemaByte = ReadWriteIOUtils.readLong(buffer);
      String alias = ReadWriteIOUtils.readString(buffer);
      Map<String, String> props = ReadWriteIOUtils.readMap(buffer);

      return paddingMeasurement(nodeName, tagIndex, schemaByte, alias, props);
    }
  }

  // region Getter and Setter to Record Buffer
  /** These methods need a buffer whose position is ready to read. */
  public static short getRecordLength(ByteBuffer recBuf) {
    int oriPos = recBuf.position();
    recBuf.position(oriPos + LENGTH_OFFSET);
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
    recBuf.position(oriPos + SEG_ADDRESS_OFFSET);
    long addr = ReadWriteIOUtils.readLong(recBuf);
    recBuf.position(oriPos);
    return addr;
  }

  /** return as: [dataType, encoding, compression, preDelete] */
  public static byte[] getMeasStatsBytes(ByteBuffer recBuf) {
    byte[] res = new byte[4];
    int oriPos = recBuf.position();
    recBuf.position(oriPos + SCHEMA_OFFSET);
    long statusBytes = ReadWriteIOUtils.readLong(recBuf);
    res[0] = (byte) (statusBytes >>> 16 & 0xffL);
    res[1] = (byte) (statusBytes >>> 8 & 0xffL);
    res[2] = (byte) (statusBytes & 0xffL);
    res[3] = (byte) (statusBytes >>> 24 & 0xffL);
    recBuf.position(oriPos);
    return res;
  }

  public static boolean getAlignment(ByteBuffer recBuf) {
    int oriPos = recBuf.position();
    recBuf.position(oriPos + INTERNAL_BITFLAG_OFFSET);
    byte flag = ReadWriteIOUtils.readByte(recBuf);
    recBuf.position(oriPos);
    return isAligned(flag);
  }

  public static String getRecordAlias(ByteBuffer recBuf) {
    int oriPos = recBuf.position();
    if (ReadWriteIOUtils.readByte(recBuf) != MEASUREMENT_TYPE) {
      recBuf.position(oriPos);
      return null;
    }
    recBuf.position(oriPos + ALIAS_OFFSET);
    String alias = ReadWriteIOUtils.readString(recBuf);
    recBuf.position(oriPos);
    return alias;
  }

  public static void updateSegAddr(ByteBuffer recBuf, long newSegAddr) {
    int oriPos = recBuf.position();
    recBuf.position(oriPos + SEG_ADDRESS_OFFSET);
    ReadWriteIOUtils.write(newSegAddr, recBuf);
    recBuf.position(oriPos);
  }

  // endregion

  @TestOnly
  public static String buffer2String(ByteBuffer buffer) throws MetadataException {
    StringBuilder builder = new StringBuilder("[");
    IMNode node = buffer2Node("unspecified", buffer);
    if (node.isMeasurement()) {
      builder.append("measurementNode, ");
      builder.append(
          String.format(
              "alias: %s, ",
              node.getAsMeasurementMNode().getAlias() == null
                  ? ""
                  : node.getAsMeasurementMNode().getAlias()));
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
    return node.getOffset();
  }

  /** Including schema and pre-delete flag of a measurement, could be expanded further. */
  private static long convertMeasStat2Long(IMeasurementMNode node) {
    byte dataType = node.getSchema().getTypeInByte();
    byte encoding = node.getSchema().getEncodingType().serialize();
    byte compressor = node.getSchema().getCompressor().serialize();
    byte preDelete = (byte) (node.getAsMeasurementMNode().isPreDeleted() ? 0x01 : 0x00);

    return (preDelete << 24 | dataType << 16 | encoding << 8 | compressor);
  }

  private static IMNode paddingMeasurement(
      String nodeName, long tagIndex, long statsBytes, String alias, Map<String, String> props) {
    byte preDel = (byte) (statsBytes >>> 24);
    byte dataType = (byte) (statsBytes >>> 16);
    byte encoding = (byte) ((statsBytes >>> 8) & 0xffL);
    byte compressor = (byte) (statsBytes & 0xffL);

    IMeasurementSchema schema =
        new MeasurementSchema(
            nodeName,
            TSDataType.values()[dataType],
            TSEncoding.values()[encoding],
            CompressionType.deserialize(compressor),
            props);

    IMNode res = MeasurementMNode.getMeasurementMNode(null, nodeName, schema, alias);
    res.getAsMeasurementMNode().setOffset(tagIndex);

    if (preDel > 0) {
      res.getAsMeasurementMNode().setPreDeleted(true);
    }

    return res;
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
