package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class MetaFile implements MetaFileAccess {

  private final MTreeFile mTreeFile;
  private final MeasurementFile measurementFile;

  public MetaFile(String mTreeFilePath, String measurementFilePath) throws IOException {
    mTreeFile = new MTreeFile(mTreeFilePath);
    measurementFile = new MeasurementFile(measurementFilePath);
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
    if (mNode.isMeasurement()) {
      measurementFile.readData((MeasurementMNode) mNode);
    } else {
      mNode = mTreeFile.readData(mNode);
    }
    return mNode;
  }

  @Override
  public void write(MNode mNode) throws IOException {
    if (mNode.isMeasurement()) {
      measurementFile.write((MeasurementMNode) mNode);
    } else {
      mTreeFile.write(mNode);
    }
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
    measurementFile.close();
  }

  @Override
  public void sync() throws IOException {
    mTreeFile.sync();
    measurementFile.sync();
  }

  public MNode readMNode(String path) throws IOException {
    MNode mNode = mTreeFile.read(path);
    if (mNode.isMeasurement()) {
      measurementFile.readData((MeasurementMNode) mNode);
    }
    return mNode;
  }

  public MNode readMNode(long position, boolean isMeasurement) throws IOException {
    if (isMeasurement) {
      return measurementFile.read(position);
    } else {
      return mTreeFile.read(position);
    }
  }

  public MNode readRecursively(long position) throws IOException {
    MNode mNode = mTreeFile.read(position);
    for (MNode child : mNode.getChildren().values()) {
      readRecursively(child);
    }
    return mNode;
  }

  public void readRecursively(MNode mNode) throws IOException {
    mNode = readData(mNode);
    for (MNode child : mNode.getChildren().values()) {
      readRecursively(child);
    }
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
      if (mNode.isMeasurement()) {
        mNode.setPosition(measurementFile.getFreePos());
      } else {
        if (mNode.getName().equals("root")) {
          mNode.setPosition(mTreeFile.getRootPosition());
        } else {
          mNode.setPosition(mTreeFile.getFreePos());
        }
      }
    }
  }
}
