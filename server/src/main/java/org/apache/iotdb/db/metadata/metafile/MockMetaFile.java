package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MockMetaFile implements MetaFileAccess {

  private final Map<String, MNode> mockFile = new ConcurrentHashMap<>();

  public MockMetaFile(String mTreeFilePath, String measurementFilePath) {}

  @Override
  public MNode read(PartialPath path) throws IOException {
    MNode mNode=mockFile.get(path.getFullPath());
    for(String childName:mNode.getChildren().keySet()){
      mNode.evictChild(childName);
    }
    return mNode;
  }

  @Override
  public MNode read(long position, boolean isMeasurement) throws IOException {
    return null;
  }

  @Override
  public MNode readData(MNode mNode) throws IOException {
    return null;
  }

  @Override
  public void write(MNode mNode) throws IOException {
    mNode.setModified(false);
    mNode.setPosition(-1);
    mockFile.put(mNode.getFullPath(), mNode);
    if (mNode instanceof MeasurementMNode) {
      mockFile.put(
          mNode.getParent().getFullPath()
              + IoTDBConstant.PATH_SEPARATOR
              + ((MeasurementMNode) mNode).getAlias(),
          mNode);
    }
  }

  @Override
  public void write(Collection<MNode> mNodes) throws IOException {}

  @Override
  public void remove(PartialPath path) throws IOException {
    mockFile.remove(path.getFullPath());
  }

  @Override
  public void remove(long position) throws IOException {}

  @Override
  public void close() throws IOException {}

  @Override
  public void sync() throws IOException {}
}
