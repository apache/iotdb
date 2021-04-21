package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.PersistenceMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class MTreeFile {

  private static final int HEADER_LENGTH = 64;
  private static final int NODE_LENGTH = 512*2;

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

  public MNode read(String path) throws IOException {
    String[] nodes = path.split("\\.");
    if (!nodes[0].equals("root")) {
      return null;
    }
    MNode mNode = read(rootPosition);
    for (int i = 1; i < nodes.length; i++) {
      if(!mNode.hasChild(nodes[i])){
        return null;
      }
      mNode = read(mNode.getChild(nodes[i]).getPersistenceInfo().getPosition());
    }
    return mNode;
  }

  public MNode readRecursively(long position) throws IOException{
    MNode mNode=new MNode(null,null);
    mNode.setPersistenceInfo(new PersistenceMNode(position));
    readDataRecursively(mNode);
    return mNode;
  }

  public void readDataRecursively(MNode mNode) throws IOException {
    MNode mnode = readData(mNode);
    for (MNode child : mnode.getChildren().values()) {
      readDataRecursively(child);
    }
  }

  public MNode readData(MNode mNode) throws IOException {
    PersistenceInfo persistenceInfo=mNode.getPersistenceInfo();
    long position = persistenceInfo.getPosition();
    ByteBuffer dataBuffer = readBytesFromFile(position);
    int type = dataBuffer.get();
    if(type>3){
      throw new IOException("wrong position for node read");
    }else if(type>1){
      if (type == 2) {
        if (!mNode.isStorageGroup()) {
          mNode = new StorageGroupMNode(mNode.getParent(), mNode.getName(), 0);
        }
        readStorageGroupMNode(dataBuffer, (StorageGroupMNode) mNode);
      } else {
        if(!mNode.isMeasurement()){
          mNode = new MeasurementMNode(mNode.getParent(), mNode.getName(), null,null);
        }
        readMeasurementGroupMNode(dataBuffer,(MeasurementMNode) mNode);
      }
      mNode.getParent().getChildren().replace(mNode.getName(),mNode);
      mNode.setPersistenceInfo(persistenceInfo);
    }else {
      readMNode(dataBuffer, mNode);
    }
    return mNode;
  }

  public MNode read(long position) throws IOException {
    ByteBuffer dataBuffer = readBytesFromFile(position);
    int type = dataBuffer.get();
    MNode mNode;
    if (type == 2) {
      mNode = new StorageGroupMNode(null, null, 0);
      readStorageGroupMNode(dataBuffer, (StorageGroupMNode) mNode);
    } else if(type==3){
      mNode=new MeasurementMNode(null,null,null,null);
      readMeasurementGroupMNode(dataBuffer,(MeasurementMNode) mNode);
    }else {
      mNode = new MNode(null, null);
      readMNode(dataBuffer, mNode);
    }
    mNode.setPersistenceInfo(new PersistenceMNode(position));

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

  private void readMeasurementGroupMNode(ByteBuffer dataBuffer, MeasurementMNode mNode){
    mNode.setName(ReadWriteIOUtils.readVarIntString(dataBuffer));
    String alias=ReadWriteIOUtils.readVarIntString(dataBuffer);
    mNode.setAlias("".equals(alias)?null:alias);
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

    readChildren(mNode, dataBuffer);
  }

  private void readChildren(MNode mNode, ByteBuffer byteBuffer) {
    String name;
    name = ReadWriteIOUtils.readVarIntString(byteBuffer);
    Map<Long, String> children=new HashMap<>();
    while (name != null && !name.equals("")) {
      children.put(byteBuffer.getLong(),name);
      name = ReadWriteIOUtils.readVarIntString(byteBuffer);
    }
    Map<Long, String> aliasChildren=new HashMap<>();
    name = ReadWriteIOUtils.readVarIntString(byteBuffer);
    while (name != null && !name.equals("")) {
      aliasChildren.put(byteBuffer.getLong(),name);
      name = ReadWriteIOUtils.readVarIntString(byteBuffer);
    }

    MNode child;
    for(long position:children.keySet()){
      if(aliasChildren.containsKey(position)){
        child=new MeasurementMNode(mNode,children.get(position),null,aliasChildren.get(position));
        mNode.addAlias(aliasChildren.get(position),mNode);
      }else {
        child = new MNode(mNode, children.get(position));
      }
      child.setPersistenceInfo(new PersistenceMNode(position));
      mNode.addChild(child.getName(), child);
    }
  }

  public void write(MNode mNode) throws IOException {
    if (mNode == null) {
      throw new IOException("MNode is null and cannot be persist.");
    }

    Map<Long, ByteBuffer> mNodeBytes = serializeMNode(mNode);
    if (mNodeBytes == null) {
      throw new IOException("Too large Node to persist.");
    }
    ByteBuffer byteBuffer;
    for (long position : mNodeBytes.keySet()) {
      if(position<headerLength){
        throw new IOException("illegal position for node");
      }
      byteBuffer = mNodeBytes.get(position);
      fileAccess.writeBytes(position, byteBuffer);
    }
  }

  public void writeRecursively(MNode mNode) throws IOException {
    write(mNode);
    for (MNode child : mNode.getChildren().values()) {
      writeRecursively(child);
    }
    write(mNode);
  }

  private int evaluateNodeLength(MNode mNode) {
    int length = 0;
    length += ReadWriteForEncodingUtils.varIntSize(mNode.getName().length()) + mNode.getName().length(); // string.length()==string.getBytes().length

    // children
    for (String childName : mNode.getChildren().keySet()) {
      length += ReadWriteForEncodingUtils.varIntSize(childName.length()) + childName.length() + 8; // child name and child position
    }
    length += 1; // children end tag
    for(String alias:mNode.getAliasChildren().keySet()){
      length += ReadWriteForEncodingUtils.varIntSize(alias.length()) + alias.length() + 8; // child alias and child position
    }
    length +=1; // aliasChildren end tag

    if (mNode.isStorageGroup()) {
      length += 8; // TTL
    }

    if(mNode.isMeasurement()){
      MeasurementMNode measurementMNode=(MeasurementMNode)mNode;
      String alias=measurementMNode.getAlias(); // alias
      if(alias==null){
        length += ReadWriteForEncodingUtils.varIntSize(0);
      }else {
        length += ReadWriteForEncodingUtils.varIntSize(alias.length())+alias.length();
      }
      length += 8; // offset
      length += 3; // type, encoding, compressor
      if(measurementMNode.getSchema().getProps()!=null){
        for(Map.Entry<String,String> entry:measurementMNode.getSchema().getProps().entrySet()){
          length += ReadWriteForEncodingUtils.varIntSize(entry.getKey().length())+entry.getKey().length();
          length += ReadWriteForEncodingUtils.varIntSize(entry.getValue().length())+entry.getValue().length();
        }
      }
      length += 1; // end tag of props
    }
    return length;
  }

  private Map<Long, ByteBuffer> serializeMNode(MNode mNode) throws IOException {

    if (mNode.getPersistenceInfo() == null) {
      if (mNode.getName().equals("root")) {
        mNode.setPersistenceInfo(new PersistenceMNode(rootPosition));
      } else {
        mNode.setPersistenceInfo(new PersistenceMNode(getFreePos()));
      }
    }

    int mNodeLength = evaluateNodeLength(mNode);
    int bufferNum =
        (mNodeLength / (nodeLength - 17)) + ((mNodeLength % (nodeLength - 17)) == 0 ? 0 : 1);
    if (bufferNum > 15) {
      return null;
    }
    ByteBuffer[] bufferList = new ByteBuffer[bufferNum];
    Map<Long, ByteBuffer> result = new HashMap<>(bufferNum);

    byte bitmap = (byte) bufferNum;
    ByteBuffer dataBuffer = ByteBuffer.allocate(mNodeLength);
    if (mNode.isStorageGroup()) {
      serializeStorageGroupMNodeData((StorageGroupMNode) mNode, dataBuffer);
      bitmap = (byte) (0xA0 | bitmap);
    } else if(mNode.isMeasurement()){
      serializeMeasurementMNodeData((MeasurementMNode) mNode, dataBuffer);
      bitmap=(byte)(0xB0|bitmap);
    } else {
      serializeMNodeData(mNode,dataBuffer);
      if (mNode.getName().equals("root")) {
        bitmap = (byte) (0x90 | bitmap);
      } else {
        bitmap = (byte) (0x80 | bitmap);
      }
    }

    bufferList[0] = ByteBuffer.allocate(nodeLength);
    bufferList[0].put(bitmap);
    MNode parent = mNode.getParent();
    long prePos = parent == null ? 0 : parent.getPersistenceInfo().getPosition();
    bufferList[0].putLong(prePos);
    long extensionPos = (0 == bufferNum - 1) ? 0 : getFreePos();
    bufferList[0].putLong(extensionPos);
    dataBuffer.limit(Math.min(dataBuffer.position() + nodeLength - 17, dataBuffer.capacity()));
    bufferList[0].put(dataBuffer);
    bufferList[0].position(0);
    long currentPos = mNode.getPersistenceInfo().getPosition();
    result.put(currentPos, bufferList[0]);

    for (int i = 1; i < bufferNum; i++) {
      prePos = currentPos;
      currentPos = extensionPos;
      bufferList[i] = ByteBuffer.allocate(nodeLength);
      result.put(currentPos, bufferList[i]);
      bitmap = (byte) (0xC0 | (bufferNum - i));
      bufferList[i].put(bitmap);
      bufferList[i].putLong(prePos);
      extensionPos = (i == bufferNum - 1) ? 0 : getFreePos();
      bufferList[i].putLong(extensionPos);
      dataBuffer.limit(Math.min(dataBuffer.position() + nodeLength - 17, dataBuffer.capacity()));
      bufferList[i].put(dataBuffer);
      bufferList[i].position(0);
    }

    return result;
  }

  private void serializeMNodeData(MNode mNode, ByteBuffer dataBuffer) {
    ReadWriteIOUtils.writeVar(mNode.getName(), dataBuffer);
    serializeChildren(mNode.getChildren(), dataBuffer);
    serializeChildren(mNode.getAliasChildren(),dataBuffer);
    dataBuffer.flip();
  }

  private void serializeStorageGroupMNodeData(StorageGroupMNode mNode, ByteBuffer dataBuffer) {
    ReadWriteIOUtils.writeVar(mNode.getName(), dataBuffer);
    dataBuffer.putLong(mNode.getDataTTL());
    serializeChildren(mNode.getChildren(), dataBuffer);
    serializeChildren(mNode.getAliasChildren(),dataBuffer);
    dataBuffer.flip();
  }

  private void serializeMeasurementMNodeData(MeasurementMNode mNode, ByteBuffer dataBuffer){
    ReadWriteIOUtils.writeVar(mNode.getName(), dataBuffer);
    ReadWriteIOUtils.writeVar(mNode.getAlias()==null?"":mNode.getAlias(), dataBuffer);
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
    serializeChildren(mNode.getChildren(), dataBuffer);
    serializeChildren(mNode.getAliasChildren(),dataBuffer);
    dataBuffer.flip();
  }

  private void serializeChildren(Map<String, MNode> children, ByteBuffer dataBuffer) {
    PersistenceInfo persistenceInfo;
    for (MNode child : children.values()) {
      ReadWriteIOUtils.writeVar(child.getName(), dataBuffer);
      persistenceInfo=child.getPersistenceInfo();
      dataBuffer.putLong(persistenceInfo==null?0:persistenceInfo.getPosition());
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

  public void remove(long position) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(17);
    while (position!=0){
      fileAccess.readBytes(position, buffer);
      byte bitmap=buffer.get();
      long pre=buffer.getLong();
      long extension=buffer.getLong();

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

      position=extension;
    }
  }

  public void clear() throws IOException {}

  public void sync() throws IOException {
    writeMetaFileHeader();
    fileAccess.sync();
  }

  public void close() throws IOException {
    sync();
    fileAccess.close();
  }
}
