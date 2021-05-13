package org.apache.iotdb.db.metadata.metadisk.metafile;

import org.apache.iotdb.db.metadata.mnode.*;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class MTreeFile {

  private static final int HEADER_LENGTH = 64;
  private static final int NODE_LENGTH = 512 * 2;

  private static final int INTERNAL_MNODE_TYPE = 0;
  private static final int ROOT_MNODE_TYPE = 1;
  private static final int STORAGE_GROUP_MNODE_TYPE = 2;
  private static final int MEASUREMENT_MNODE_TYPE = 3;
  private static final int EXTENSION_TYPE = 4;

  private final SlottedFileAccess fileAccess;

  private int headerLength;
  private short nodeLength;
  private long rootPosition;
  private long firstStorageGroupPosition;
  private long firstTimeseriesPosition;
  private long firstFreePosition;
  private int mNodeCount;
  private int storageGroupCount;
  private int timeseriesCount;

  private final List<Long> freePosition = new LinkedList<>();

  public MTreeFile(String filepath) throws IOException {
    File metaFile = new File(filepath);
    boolean isNew = !metaFile.exists();
    fileAccess = new SlottedFile(filepath, HEADER_LENGTH, NODE_LENGTH);

    if (isNew) {
      initMetaFileHeader();
    } else {
      readMetaFileHeader();
      fileCheck();
    }
  }

  private void initMetaFileHeader() throws IOException {
    headerLength = HEADER_LENGTH;
    nodeLength = NODE_LENGTH;
    rootPosition = HEADER_LENGTH;
    firstStorageGroupPosition = 0;
    firstTimeseriesPosition = 0;
    firstFreePosition = rootPosition + nodeLength;
    mNodeCount = 0;
    storageGroupCount = 0;
    timeseriesCount = 0;
    writeMetaFileHeader();
  }

  private void readMetaFileHeader() throws IOException {
    ByteBuffer buffer = fileAccess.readHeader();
    headerLength = buffer.get();
    nodeLength = buffer.getShort();
    rootPosition = buffer.getLong();
    firstStorageGroupPosition = buffer.getLong();
    firstTimeseriesPosition = buffer.getLong();
    firstFreePosition = buffer.getLong();
    mNodeCount = buffer.getInt();
    storageGroupCount = buffer.getInt();
    timeseriesCount = buffer.getInt();
  }

  private void writeMetaFileHeader() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(headerLength);
    buffer.put((byte) headerLength);
    buffer.putShort(nodeLength);
    buffer.putLong(rootPosition);
    buffer.putLong(firstStorageGroupPosition);
    buffer.putLong(firstTimeseriesPosition);
    buffer.putLong(firstFreePosition);
    buffer.putInt(mNodeCount);
    buffer.putInt(storageGroupCount);
    buffer.putInt(timeseriesCount);
    buffer.position(0);
    fileAccess.writeHeader(buffer);
  }

  private void fileCheck() throws IOException {
    PersistenceInfo persistenceInfo = PersistenceInfo.createPersistenceInfo(rootPosition);
    MNode mNode = read(persistenceInfo);
  }

  public MNode read(String path) throws IOException {
    String[] nodes = path.split("\\.");
    if (!nodes[0].equals("root")) {
      return null;
    }
    MNode mNode = read(PersistenceInfo.createPersistenceInfo(rootPosition));
    for (int i = 1; i < nodes.length; i++) {
      if (!mNode.hasChild(nodes[i])) {
        return null;
      }
      mNode = read(mNode.getChild(nodes[i]).getPersistenceInfo());
    }
    return mNode;
  }

  public MNode readRecursively(PersistenceInfo persistenceInfo) throws IOException {
    MNode mNode = read(persistenceInfo);
    Map<String, MNode> children = mNode.getChildren();
    MNode child;
    for (Map.Entry<String, MNode> entry : children.entrySet()) {
      child = entry.getValue();
      child = readRecursively(child.getPersistenceInfo());
      entry.setValue(child);
      child.setParent(mNode);
    }
    return mNode;
  }

  public MNode read(PersistenceInfo persistenceInfo) throws IOException {
    long position = persistenceInfo.getPosition();
    ByteBuffer dataBuffer = readBytesFromFile(position);
    int type = dataBuffer.get();
    MNode mNode;
    switch (type) {
      case INTERNAL_MNODE_TYPE: // normal InternalMNode
        mNode = new InternalMNode(null, null);
        readMNode(dataBuffer, mNode);
        break;
      case ROOT_MNODE_TYPE: // root
        mNode = new InternalMNode(null, "root");
        readMNode(dataBuffer, mNode);
        break;
      case STORAGE_GROUP_MNODE_TYPE: // StorageGroupMNode
        mNode = new StorageGroupMNode(null, null, 0);
        readStorageGroupMNode(dataBuffer, (StorageGroupMNode) mNode);
        break;
      case MEASUREMENT_MNODE_TYPE: // MeasurementMNode
        mNode = new MeasurementMNode(null, null, null, null);
        readMeasurementMNode(dataBuffer, (MeasurementMNode) mNode);
        break;
      default:
        throw new IOException("file corrupted");
    }
    mNode.setPersistenceInfo(persistenceInfo);

    return mNode;
  }

  private ByteBuffer readBytesFromFile(long position) throws IOException {
    if (position < headerLength) {
      throw new IOException("wrong node position");
    }

    ByteBuffer buffer = ByteBuffer.allocate(nodeLength);
    fileAccess.readBytes(position, buffer);

    byte bitmap = buffer.get();
    if ((bitmap & 0x80) == 0) {
      throw new IOException("file corrupted");
    }
    int type = (bitmap & 0x70) >> 4;
    if (type > 3) {
      throw new IOException("file corrupted");
    }
    int num = bitmap & 0x0f;

    long parentPosition = buffer.getLong();
    long prePosition;
    long extendPosition = buffer.getLong();

    ByteBuffer dataBuffer =
        ByteBuffer.allocate(1 + num * (nodeLength - 17)); // node type + node data
    dataBuffer.put((byte) type);
    dataBuffer.put(buffer);
    for (int i = 1; i < num; i++) {
      buffer.clear();
      fileAccess.readBytes(extendPosition, buffer);
      bitmap = buffer.get();
      if ((bitmap & 0x80) == 0) {
        throw new IOException("file corrupted");
      }
      if (((bitmap & 0x70) >> 4) < 4) {
        // 地址空间对应非扩展节点
        throw new IOException("File corrupted");
      }
      prePosition = buffer.getLong();
      extendPosition = buffer.getLong();
      dataBuffer.put(buffer);
    }
    dataBuffer.flip();
    return dataBuffer;
  }

  private void readMNode(ByteBuffer dataBuffer, MNode mNode) {
    mNode.setName(ReadWriteIOUtils.readVarIntString(dataBuffer));
    readChildren(mNode, dataBuffer);
  }

  private void readStorageGroupMNode(ByteBuffer dataBuffer, StorageGroupMNode mNode) {
    mNode.setName(ReadWriteIOUtils.readVarIntString(dataBuffer));
    mNode.setDataTTL(dataBuffer.getLong());
    readChildren(mNode, dataBuffer);
  }

  private void readMeasurementMNode(ByteBuffer dataBuffer, MeasurementMNode mNode) {
    mNode.setName(ReadWriteIOUtils.readVarIntString(dataBuffer));
    String alias = ReadWriteIOUtils.readVarIntString(dataBuffer);
    mNode.setAlias("".equals(alias) ? null : alias);
    mNode.setOffset(dataBuffer.getLong());

    byte type = dataBuffer.get();
    byte encoding = dataBuffer.get();
    byte compressor = dataBuffer.get();
    Map<String, String> props = new HashMap<>();
    String key;
    key = ReadWriteIOUtils.readVarIntString(dataBuffer);
    while (key != null && !key.equals("")) {
      props.put(key, ReadWriteIOUtils.readVarIntString(dataBuffer));
      key = ReadWriteIOUtils.readVarIntString(dataBuffer);
    }
    MeasurementSchema schema =
        new MeasurementSchema(
            mNode.getName(),
            TSDataType.deserialize(type),
            TSEncoding.deserialize(encoding),
            CompressionType.deserialize(compressor),
            props.size() == 0 ? null : props);
    mNode.setSchema(schema);

    long timestamp = dataBuffer.getLong();
    if (timestamp != -1) {
      TSDataType dataType = schema.getType();
      TsPrimitiveType value;
      switch (dataType) {
        case BOOLEAN:
          value = TsPrimitiveType.getByType(dataType, dataBuffer.get() == 1);
          break;
        case INT32:
          value = TsPrimitiveType.getByType(dataType, dataBuffer.getInt());
          break;
        case INT64:
          value = TsPrimitiveType.getByType(dataType, dataBuffer.getLong());
          break;
        case FLOAT:
          value = TsPrimitiveType.getByType(dataType, dataBuffer.getFloat());
          break;
        case DOUBLE:
          value = TsPrimitiveType.getByType(dataType, dataBuffer.getDouble());
          break;
        case TEXT:
          byte[] content = new byte[dataBuffer.getInt()];
          dataBuffer.get(content);
          value = TsPrimitiveType.getByType(dataType, new Binary(content));
          break;
        default:
          throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
      }
      TimeValuePair lastCache = new TimeValuePair(timestamp, value);
      mNode.updateCachedLast(lastCache, false, 0L);
    }

    readChildren(mNode, dataBuffer);
  }

  private void readChildren(MNode mNode, ByteBuffer byteBuffer) {
    String name;
    name = ReadWriteIOUtils.readVarIntString(byteBuffer);
    Map<Long, String> children = new HashMap<>();
    while (name != null && !name.equals("")) {
      children.put(byteBuffer.getLong(), name);
      name = ReadWriteIOUtils.readVarIntString(byteBuffer);
    }
    Map<Long, String> aliasChildren = new HashMap<>();
    name = ReadWriteIOUtils.readVarIntString(byteBuffer);
    while (name != null && !name.equals("")) {
      aliasChildren.put(byteBuffer.getLong(), name);
      name = ReadWriteIOUtils.readVarIntString(byteBuffer);
    }

    MNode child;
    for (long position : children.keySet()) {
      child = new PersistenceMNode(position);
      mNode.addChild(children.get(position), child);
      if (aliasChildren.containsKey(position)) {
        mNode.addAlias(aliasChildren.get(position), child);
      }
    }
  }

  public void write(MNode mNode) throws IOException {
    if (mNode == null) {
      throw new IOException("MNode is null and cannot be persist.");
    }
    if (!mNode.isLoaded()) {
      return;
    }

    Map<Long, ByteBuffer> mNodeBytes = serializeMNode(mNode);
    ByteBuffer byteBuffer;
    for (long position : mNodeBytes.keySet()) {
      if (position < headerLength) {
        throw new IOException("illegal position for node");
      }
      byteBuffer = mNodeBytes.get(position);
      fileAccess.writeBytes(position, byteBuffer);
    }
  }

  public void writeRecursively(MNode mNode) throws IOException {
    for (MNode child : mNode.getChildren().values()) {
      writeRecursively(child);
    }
    write(mNode);
  }

  private int evaluateNodeLength(MNode mNode) {
    int length = 0;
    length +=
        ReadWriteForEncodingUtils.varIntSize(mNode.getName().length())
            + mNode.getName().length(); // string.length()==string.getBytes().length

    // children
    length += evaluateChildrenLength(mNode.getChildren());
    // alias children
    length += evaluateChildrenLength(mNode.getAliasChildren());

    if (mNode.isStorageGroup()) {
      length += 8; // TTL
    }

    if (mNode.isMeasurement()) {
      length += evaluateMeasurementDataLength((MeasurementMNode) mNode);
    }
    return length;
  }

  private int evaluateChildrenLength(Map<String, MNode> children) {
    int length = 0;
    for (String childName : children.keySet()) {
      length +=
          ReadWriteForEncodingUtils.varIntSize(childName.length())
              + childName.length()
              + 8; // child name and child position
    }
    length += 1; // children end tag
    return length;
  }

  private int evaluateMeasurementDataLength(MeasurementMNode measurementMNode) {
    int length = 0;
    String alias = measurementMNode.getAlias(); // alias
    if (alias == null) {
      length += ReadWriteForEncodingUtils.varIntSize(0);
    } else {
      length += ReadWriteForEncodingUtils.varIntSize(alias.length()) + alias.length();
    }
    length += 8; // offset
    length += 3; // type, encoding, compressor
    if (measurementMNode.getSchema().getProps() != null) {
      for (Map.Entry<String, String> entry : measurementMNode.getSchema().getProps().entrySet()) {
        length +=
            ReadWriteForEncodingUtils.varIntSize(entry.getKey().length()) + entry.getKey().length();
        length +=
            ReadWriteForEncodingUtils.varIntSize(entry.getValue().length())
                + entry.getValue().length();
      }
    }
    length += 1; // end tag of props

    // lastCache
    length += 8; // timestamp, -1 means lastCache is null
    if (measurementMNode.getCachedLast() != null) {
      TSDataType dataType = measurementMNode.getSchema().getType();
      switch (dataType) { // value
        case BOOLEAN:
          length += 1;
          break;
        case INT32:
          length += 4;
          break;
        case INT64:
          length += 8;
          break;
        case FLOAT:
          length += 4;
          break;
        case DOUBLE:
          length += 8;
          break;
        case TEXT:
          length +=
              4
                  + measurementMNode
                      .getCachedLast()
                      .getValue()
                      .getBinary()
                      .getLength(); // length + data
          break;
        default:
          throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
      }
    }

    return length;
  }

  private Map<Long, ByteBuffer> serializeMNode(MNode mNode) throws IOException {
    if (mNode.getPersistenceInfo() == null) {
      if (mNode.getName().equals("root")) {
        mNode.setPersistenceInfo(PersistenceInfo.createPersistenceInfo(rootPosition));
      } else {
        mNode.setPersistenceInfo(PersistenceInfo.createPersistenceInfo(getFreePos()));
      }
    }
    int mNodeLength = evaluateNodeLength(mNode);
    int bufferNum =
        (mNodeLength / (nodeLength - 17)) + ((mNodeLength % (nodeLength - 17)) == 0 ? 0 : 1);
    if (bufferNum > 15) {
      throw new IOException(
          "Too large Node, "
              + mNode.getFullPath()
              + ", to persist. Node data size "
              + (mNodeLength + bufferNum * 17)
              + ", Node children num "
              + mNode.getChildren().size());
    }
    byte bitmap = (byte) bufferNum;
    ByteBuffer dataBuffer = ByteBuffer.allocate(mNodeLength);
    if (mNode.isStorageGroup()) {
      serializeStorageGroupMNodeData((StorageGroupMNode) mNode, dataBuffer);
      bitmap = (byte) (0xA0 | bitmap);
    } else if (mNode.isMeasurement()) {
      serializeMeasurementMNodeData((MeasurementMNode) mNode, dataBuffer);
      bitmap = (byte) (0xB0 | bitmap);
    } else {
      serializeMNodeData(mNode, dataBuffer);
      if (mNode.getName().equals("root")) {
        bitmap = (byte) (0x90 | bitmap);
      } else {
        bitmap = (byte) (0x80 | bitmap);
      }
    }
    return splitBytes(dataBuffer, bufferNum, bitmap, mNode);
  }

  private Map<Long, ByteBuffer> splitBytes(
      ByteBuffer dataBuffer, int bufferNum, byte bitmap, MNode mNode) throws IOException {
    Map<Long, ByteBuffer> result = new HashMap<>(bufferNum);
    ByteBuffer buffer;
    MNode parent = mNode.getParent();
    long currentPos =
        (parent == null || !parent.isPersisted()) ? 0 : parent.getPersistenceInfo().getPosition();
    long prePos;
    long extensionPos = mNode.getPersistenceInfo().getPosition();
    for (int i = 0; i < bufferNum; i++) {
      prePos = currentPos;
      currentPos = extensionPos;
      buffer = ByteBuffer.allocate(nodeLength);
      result.put(currentPos, buffer);
      if (i > 0) {
        bitmap = (byte) (0xC0 | (bufferNum - i));
      }
      buffer.put(bitmap);
      buffer.putLong(prePos);
      extensionPos = (i == bufferNum - 1) ? 0 : getFreePos();
      buffer.putLong(extensionPos);
      dataBuffer.limit(Math.min(dataBuffer.position() + nodeLength - 17, dataBuffer.capacity()));
      buffer.put(dataBuffer);
      buffer.position(0);
    }
    return result;
  }

  private void serializeMNodeData(MNode mNode, ByteBuffer dataBuffer) {
    ReadWriteIOUtils.writeVar(mNode.getName(), dataBuffer);
    serializeChildren(mNode.getChildren(), dataBuffer);
    serializeChildren(mNode.getAliasChildren(), dataBuffer);
    dataBuffer.flip();
  }

  private void serializeStorageGroupMNodeData(StorageGroupMNode mNode, ByteBuffer dataBuffer) {
    ReadWriteIOUtils.writeVar(mNode.getName(), dataBuffer);
    dataBuffer.putLong(mNode.getDataTTL());
    serializeChildren(mNode.getChildren(), dataBuffer);
    serializeChildren(mNode.getAliasChildren(), dataBuffer);
    dataBuffer.flip();
  }

  private void serializeMeasurementMNodeData(MeasurementMNode mNode, ByteBuffer dataBuffer) {
    ReadWriteIOUtils.writeVar(mNode.getName(), dataBuffer);
    ReadWriteIOUtils.writeVar(mNode.getAlias() == null ? "" : mNode.getAlias(), dataBuffer);
    dataBuffer.putLong(mNode.getOffset());
    MeasurementSchema schema = mNode.getSchema();
    dataBuffer.put(schema.getType().serialize());
    dataBuffer.put(schema.getEncodingType().serialize());
    dataBuffer.put(schema.getCompressor().serialize());
    if (schema.getProps() != null) {
      for (Map.Entry<String, String> entry : schema.getProps().entrySet()) {
        ReadWriteIOUtils.writeVar(entry.getKey(), dataBuffer);
        ReadWriteIOUtils.writeVar(entry.getValue(), dataBuffer);
      }
    }
    dataBuffer.put((byte) 0);

    TimeValuePair lastCache = mNode.getCachedLast();
    if (lastCache != null) {
      dataBuffer.putLong(lastCache.getTimestamp());
      TSDataType dataType = schema.getType();
      TsPrimitiveType value = lastCache.getValue();
      switch (dataType) {
        case BOOLEAN:
          dataBuffer.put((byte) (value.getBoolean() ? 1 : 0));
          break;
        case INT32:
          dataBuffer.putInt(value.getInt());
          break;
        case INT64:
          dataBuffer.putLong(value.getLong());
          break;
        case FLOAT:
          dataBuffer.putFloat(value.getFloat());
          break;
        case DOUBLE:
          dataBuffer.putDouble(value.getDouble());
          break;
        case TEXT:
          dataBuffer.putInt(value.getBinary().getLength());
          dataBuffer.put(value.getBinary().getValues());
          break;
        default:
          throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
      }
    } else {
      dataBuffer.putLong(-1);
    }

    serializeChildren(mNode.getChildren(), dataBuffer);
    serializeChildren(mNode.getAliasChildren(), dataBuffer);
    dataBuffer.flip();
  }

  private void serializeChildren(Map<String, MNode> children, ByteBuffer dataBuffer) {
    for (String childName : children.keySet()) {
      ReadWriteIOUtils.writeVar(childName, dataBuffer);
      dataBuffer.putLong(children.get(childName).getPersistenceInfo().getPosition());
    }
    dataBuffer.put((byte) 0);
  }

  public long getFreePos() throws IOException {
    if (freePosition.size() != 0) {
      return freePosition.remove(0);
    }
    firstFreePosition += nodeLength;
    return firstFreePosition - nodeLength;
  }

  public long getRootPosition() {
    return rootPosition;
  }

  public void remove(PersistenceInfo persistenceInfo) throws IOException {
    long position = persistenceInfo.getPosition();
    ByteBuffer buffer = ByteBuffer.allocate(17);
    while (position != 0) {
      fileAccess.readBytes(position, buffer);
      byte bitmap = buffer.get();
      long pre = buffer.getLong();
      long extension = buffer.getLong();

      buffer.clear();
      buffer.put((byte) (0));
      if (freePosition.size() == 0) {
        buffer.putLong(fileAccess.getFileLength());
      } else {
        buffer.putLong(freePosition.get(0));
      }
      buffer.flip();
      fileAccess.writeBytes(position, buffer);
      freePosition.add(0, position);

      position = extension;
    }
  }

  public void clear() throws IOException {}

  public void sync() throws IOException {
    writeMetaFileHeader();
    fileAccess.sync();
  }

  public void close() throws IOException {
    fileAccess.close();
  }
}
