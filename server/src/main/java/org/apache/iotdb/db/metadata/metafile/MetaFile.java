package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;

import java.io.IOException;
import java.util.*;

public class MetaFile implements MetaFileAccess {

  private final MTreeFile mTreeFile;

  public MetaFile(String mTreeFilePath) throws IOException {
    mTreeFile = new MTreeFile(mTreeFilePath);
  }

  @Override
  public MNode read(PartialPath path) throws IOException {
    return readMNode(path.toString());
  }

  @Override
  public MNode read(long position, boolean isMeasurement) throws IOException {
    return readMNode(position, isMeasurement);
  }

  @Override
  public MNode readData(MNode mNode) throws IOException {
    return mTreeFile.readData(mNode);
  }

  @Override
  public void write(MNode mNode) throws IOException {
    mTreeFile.write(mNode);
  }

  @Override
  public void write(Collection<MNode> mNodes) throws IOException {
    allocateFreePos(mNodes);
    for (MNode mNode : mNodes) {
      write(mNode);
    }
  }

  @Override
  public void remove(PartialPath path) throws IOException {}

  @Override
  public void remove(long position) throws IOException {}

  @Override
  public void close() throws IOException {
    mTreeFile.close();
  }

  @Override
  public void sync() throws IOException {
    mTreeFile.sync();
  }

  public MNode readMNode(String path) throws IOException {
    return mTreeFile.read(path);
  }

  public MNode readMNode(long position, boolean isMeasurement) throws IOException {
    return mTreeFile.read(position);
  }

  public MNode readRecursively(long position) throws IOException {
    return mTreeFile.readRecursively(position);
  }

  public void writeRecursively(MNode mNode) throws IOException {
    List<MNode> mNodeList = new LinkedList<>();
    flatten(mNode, mNodeList);
    write(mNodeList);
  }

  private void flatten(MNode mNode, Collection<MNode> mNodes) {
    mNodes.add(mNode);
    for (MNode child : mNode.getChildren().values()) {
      flatten(child, mNodes);
    }
  }

  private void allocateFreePos(Collection<MNode> mNodes) throws IOException {
    for (MNode mNode : mNodes) {
      if (mNode.getPosition() != 0) {
        continue;
      }
      if (mNode.getName().equals("root")) {
        mNode.setPosition(mTreeFile.getRootPosition());
      } else {
        mNode.setPosition(mTreeFile.getFreePos());
      }
    }
  }
}
