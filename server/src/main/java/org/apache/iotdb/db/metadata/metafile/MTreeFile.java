package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MTreeFile {

    private static final int HEADER_LENGTH = 64;
    private static final int NODE_LENGTH = 512;

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
        MNode mNode = read(rootPosition, null);
        for (int i = 1; i < nodes.length; i++) {
            if (mNode.getChild(nodes[i]) instanceof MeasurementMNode) {
                mNode=mNode.getChild(nodes[i]);
                break;
            }
            mNode = read(mNode.getChild(nodes[i]).getPosition(), mNode);
        }
        return mNode;
    }

    public MNode read(long position, MNode parent) throws IOException {
        MNode mNode = read(position);
        if (parent != null) {
            parent.addChild(mNode.getName(),mNode);
        }
        return mNode;
    }

    public MNode readRecursively(long position, MNode parent) throws IOException {
        MNode mnode = read(position, parent);
        for (MNode child : mnode.getChildren().values()) {
            if (child instanceof MeasurementMNode) {
                continue;
            }
            readRecursively(child.getPosition(), mnode);
        }
        return mnode;
    }

    public MNode read(long position) throws IOException {

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
        if(type>2){
            throw new IOException("file corrupted");
        }

        boolean isDevice = (bitmap & 0x08) != 0;

        long parentPosition = buffer.getLong();
        long prePosition;
        long extendPosition = buffer.getLong();

        int num = bitmap & 0x07;
        ByteBuffer dataBuffer = ByteBuffer.allocate(num * (nodeLength - 17));
        dataBuffer.put(buffer);
        for (int i = 1; i < num; i++) {
            buffer.clear();
            fileAccess.readBytes(extendPosition, buffer);
            bitmap = buffer.get();
            if ((bitmap & 0x80) == 0) {
                throw new IOException("file corrupted");
            }
            if (((bitmap & 0x70) >> 4) < 3) {
                // 地址空间对应非扩展节点
                throw new IOException("File corrupted");
            }
            prePosition = buffer.getLong();
            extendPosition = buffer.getLong();
            dataBuffer.put(buffer);
        }
        dataBuffer.flip();

        MNode mNode;
        if(type==2){
            mNode = readStorageGroupMNode(dataBuffer, isDevice);
        }else {
            mNode = readMNode(dataBuffer, isDevice);
        }
        mNode.setPosition(position);
        mNode.setLoaded(true);
        mNode.setModified(false);
        return mNode;
    }

    private MNode readMNode(ByteBuffer dataBuffer, boolean isDevice) {
        String name = Util.readString(dataBuffer);
        MNode mNode = new MNode(null,name);
        readChildren(mNode, dataBuffer, isDevice);
        return mNode;
    }

    private StorageGroupMNode readStorageGroupMNode(ByteBuffer dataBuffer, boolean isDevice) {
        String name = Util.readString(dataBuffer);
        long ttl=dataBuffer.getLong();
        StorageGroupMNode mNode = new StorageGroupMNode(null,name,ttl);
        readChildren(mNode, dataBuffer, isDevice);
        return mNode;
    }

    private void readChildren(MNode parent, ByteBuffer byteBuffer, boolean isDevice) {
        String name;
        MNode child;
        while (null != (name = Util.readString(byteBuffer))) {
            child = isDevice ? new MeasurementMNode(parent,name,null,null) : new MNode(parent,name);
            child.setPosition(byteBuffer.getLong());
            child.setLoaded(false);
            child.setModified(false);
            parent.addChild(name,child);
        }
    }

    public void write(MNode mNode) throws IOException {
        if (mNode == null) {
            throw new IOException("MNode is null and cannot be persist.");
        } else if (mNode instanceof MeasurementMNode) {
            throw new IOException("Cannot persist Measurement in mtree file.");
        }

        Map<Long, ByteBuffer> mNodeBytes = serializeMNode(mNode);
        if (mNodeBytes == null) {
            throw new IOException("Too large Node to persist.");
        }
        ByteBuffer byteBuffer;
        for (long position : mNodeBytes.keySet()) {
            byteBuffer = mNodeBytes.get(position);
            fileAccess.writeBytes(position, byteBuffer);
        }
        mNode.setModified(false);
    }

    public void writeRecursively(MNode mNode) throws IOException {
        write(mNode);
        for (MNode child : mNode.getChildren().values()) {
            if (child instanceof MeasurementMNode) {
                return;
            }
            writeRecursively(child);
        }
        write(mNode);
    }

    private int evaluateNodeLength(MNode mNode) {
        int length = 0;
        length += 1 + mNode.getName().length(); // string.length()==string.getBytes().length
        if (mNode.getChildren() != null) {
            // children
            for (String childName : mNode.getChildren().keySet()) {
                length += 1 + childName.length() + 8;// child name and child position
            }
            length += 1; // children end tag
        }
        if (mNode instanceof StorageGroupMNode) {
            length += 8; //TTL
        }
        return length;
    }

    private Map<Long, ByteBuffer> serializeMNode(MNode mNode) throws IOException {

        if (mNode.getPosition() == -1) {
            if (mNode.getName().equals("root")) {
                mNode.setPosition(rootPosition);
            } else {
                mNode.setPosition(getFreePos());
            }
        }

        int mNodeLength = evaluateNodeLength(mNode);
        int bufferNum = (mNodeLength / (nodeLength - 17)) + ((mNodeLength % (nodeLength - 17)) == 0 ? 0 : 1);
        if (bufferNum > 7) {
            return null;
        }
        ByteBuffer[] bufferList = new ByteBuffer[bufferNum];
        Map<Long, ByteBuffer> result = new HashMap<>();

        byte bitmap = (byte) bufferNum;
        ByteBuffer dataBuffer;
        if (mNode instanceof StorageGroupMNode) {
            dataBuffer = serializeStorageGroupMNodeData((StorageGroupMNode) mNode);
            bitmap = (byte) (0xA0 | bitmap);
        } else {
            dataBuffer = serializeMNodeData(mNode);
            if (mNode.getName().equals("root")) {
                bitmap = (byte) (0x90 | bitmap);
            } else {
                bitmap = (byte) (0x80 | bitmap);
            }
        }
        boolean isDevice = false;
        for (MNode child : mNode.getChildren().values()) {
            if (child instanceof MeasurementMNode) {
                isDevice = true;
                break;
            }
        }
        if (isDevice) {
            bitmap = (byte) (0x08 | bitmap);
        }

        bufferList[0] = ByteBuffer.allocate(nodeLength);
        bufferList[0].put(bitmap);
        MNode parent = mNode.getParent();
        bufferList[0].putLong(parent == null ? 0 : parent.getPosition());
        long extensionPos = getFreePos();
        bufferList[0].putLong(extensionPos);
        bufferList[0].put(dataBuffer);
        bufferList[0].position(0);
        result.put(mNode.getPosition(), bufferList[0]);

        for (int i = 1; i < bufferNum; i++) {
            bufferList[i] = ByteBuffer.allocate(nodeLength);
            result.put(extensionPos, bufferList[i]);
            bitmap = (byte) (0xB0 | (bufferNum - i));
            bufferList[i].put(bitmap);
            bufferList[i].putLong(extensionPos);
            extensionPos = i == bufferNum - 1 ? 0 : getFreePos();
            bufferList[i].putLong(extensionPos);
            bufferList[i].put(dataBuffer);
            bufferList[i].position(0);
        }

        return result;
    }

    private ByteBuffer serializeMNodeData(MNode mNode) {
        ByteBuffer dataBuffer = ByteBuffer.allocate(evaluateNodeLength(mNode));
        Util.writeString(dataBuffer, mNode.getName());
        for (String childName : mNode.getChildren().keySet()) {
            Util.writeString(dataBuffer, childName);
            dataBuffer.putLong(mNode.getChild(childName).getPosition());
        }
        dataBuffer.put((byte) 0);
        dataBuffer.flip();
        return dataBuffer;
    }

    private ByteBuffer serializeStorageGroupMNodeData(StorageGroupMNode mNode) {
        ByteBuffer dataBuffer = ByteBuffer.allocate(evaluateNodeLength(mNode));
        byte[] bytes = mNode.getName().getBytes();
        dataBuffer.put((byte) bytes.length);
        dataBuffer.put(bytes);
        dataBuffer.putLong(mNode.getDataTTL());
        for (String childName : mNode.getChildren().keySet()) {
            Util.writeString(dataBuffer, childName);
            dataBuffer.putLong(mNode.getChild(childName).getPosition());
        }
        dataBuffer.put((byte) 0);
        dataBuffer.flip();
        return dataBuffer;
    }

    public long getFreePos() throws IOException {
        if (freePosition.size() != 0) {
            return freePosition.remove(0);
        }
        firstFreePosition += nodeLength;
        return firstFreePosition-nodeLength;
    }

    public long getRootPosition(){
        return rootPosition;
    }

    public void remove(long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(7);
        fileAccess.readBytes(position, buffer);
        buffer.put((byte) (0));
        if (freePosition.size() == 0) {
            buffer.putLong(fileAccess.getFileLength());
        } else {
            buffer.putLong(freePosition.get(0));
        }
        buffer.flip();
        fileAccess.writeBytes(position, buffer);
        freePosition.add(0, position);
    }

    public void clear() throws IOException {
    }

    public void sync() throws IOException {
        writeMetaFileHeader();
        fileAccess.sync();
    }

    public void close() throws IOException {
        sync();
        fileAccess.close();
    }


}
