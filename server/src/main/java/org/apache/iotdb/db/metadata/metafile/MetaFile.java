package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MNodeImpl;
import org.apache.iotdb.db.metadata.mnode.PersistenceMNode;

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
  public MNode read(PersistenceInfo persistenceInfo) throws IOException {
    return readMNode(persistenceInfo);
  }

  private MNode readData(MNodeImpl mNode) throws IOException {
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
  public void remove(PersistenceInfo persistenceInfo) throws IOException {}

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

  public MNode readMNode(PersistenceInfo persistenceInfo) throws IOException {
    return mTreeFile.read(persistenceInfo.getPosition());
  }

  public MNode readRecursively(PersistenceInfo persistenceInfo) throws IOException {
    return mTreeFile.readRecursively(persistenceInfo.getPosition());
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
      if (mNode.getPersistenceInfo() != null) {
        continue;
      }
      if (mNode.getName().equals("root")) {
        mNode.setPersistenceInfo(new PersistenceMNode(mTreeFile.getRootPosition()));
      } else {
        mNode.setPersistenceInfo(new PersistenceMNode(mTreeFile.getFreePos()));
      }
    }
  }
}
