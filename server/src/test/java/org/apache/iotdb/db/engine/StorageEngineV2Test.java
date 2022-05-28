package org.apache.iotdb.db.engine;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testcontainers.shaded.com.google.common.collect.Lists;

import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DataRegion.class)
public class StorageEngineV2Test {

  private StorageEngineV2 storageEngineV2;

  @Before
  public void setUp() {
    storageEngineV2 = StorageEngineV2.getInstance();
  }

  @After
  public void after() {
    storageEngineV2 = null;
  }

  @Test
  public void testGetAllDataRegionIds() throws Exception {
    DataRegionId id1 = new DataRegionId(1);
    DataRegion rg1 = PowerMockito.mock(DataRegion.class);
    DataRegion rg2 = PowerMockito.mock(DataRegion.class);
    DataRegionId id2 = new DataRegionId(2);
    storageEngineV2.setDataRegion(id1, rg1);
    storageEngineV2.setDataRegion(id2, rg2);

    List<DataRegionId> actural = Lists.newArrayList(id1, id2);
    List<DataRegionId> expect = storageEngineV2.getAllDataRegionIds();

    Assert.assertEquals(expect.size(), actural.size());
    Assert.assertTrue(actural.containsAll(expect));
    rg1.syncDeleteDataFiles();
    rg2.syncDeleteDataFiles();
  }
}
