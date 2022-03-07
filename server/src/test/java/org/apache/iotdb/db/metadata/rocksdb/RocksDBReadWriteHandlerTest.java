package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.metadata.rocksdb.RocksDBReadWriteHandler.ROCKSDB_PATH;

public class RocksDBReadWriteHandlerTest {

  private RocksDBReadWriteHandler readWriteHandler;

  @Before
  public void setUp() throws MetadataException, RocksDBException {
    File file = new File(ROCKSDB_PATH);
    if (!file.exists()) {
      file.mkdirs();
    }
    readWriteHandler = new RocksDBReadWriteHandler();
  }

  @Test
  public void testKeyExistByTypes() throws IllegalPathException, RocksDBException {
    List<PartialPath> timeseries = new ArrayList<>();
    timeseries.add(new PartialPath("root.sg.d1.m1"));
    timeseries.add(new PartialPath("root.sg.d1.m2"));
    timeseries.add(new PartialPath("root.sg.d2.m1"));
    timeseries.add(new PartialPath("root.sg.d2.m2"));
    timeseries.add(new PartialPath("root.sg1.d1.m1"));
    timeseries.add(new PartialPath("root.sg1.d1.m2"));
    timeseries.add(new PartialPath("root.sg1.d2.m1"));
    timeseries.add(new PartialPath("root.sg1.d2.m2"));

    for (PartialPath path : timeseries) {
      String levelPath = RocksDBUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
      readWriteHandler.createNode(
          levelPath, RocksDBMNodeType.MEASUREMENT, path.getFullPath().getBytes());
    }

    for (PartialPath path : timeseries) {
      String levelPath = RocksDBUtils.getLevelPath(path.getNodes(), path.getNodeLength() - 1);
      CheckKeyResult result = readWriteHandler.keyExistByAllTypes(levelPath);
      Assert.assertTrue(result.existAnyKey());
      Assert.assertNotNull(result.getValue());
      Assert.assertEquals(path.getFullPath(), new String(result.getValue()));
    }
  }
}
