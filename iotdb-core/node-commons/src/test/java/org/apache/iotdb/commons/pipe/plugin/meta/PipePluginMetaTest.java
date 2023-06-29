package org.apache.iotdb.commons.pipe.plugin.meta;

import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PipePluginMetaTest {

  @Test
  public void testConfigNodePipePluginMetaKeeper() {
    ConfigNodePipePluginMetaKeeper keeper = new ConfigNodePipePluginMetaKeeper();
    Assert.assertFalse(keeper.containsJar("test.jar"));
    keeper.addJarNameAndMd5("test.jar", "md5");
    Assert.assertTrue(keeper.jarNameExistsAndMatchesMd5("test.jar", "md5"));

    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      keeper.processTakeSnapshot(outputStream);
      keeper.removeJarNameAndMd5IfPossible("test.jar");
      ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
      keeper.processLoadSnapshot(inputStream);
      Assert.assertTrue(keeper.jarNameExistsAndMatchesMd5("test.jar", "md5"));
    } catch (IOException e) {
      Assert.fail();
    }
  }

  @Test
  public void testDataNodePipePluginMetaKeeper() {
    DataNodePipePluginMetaKeeper keeper = new DataNodePipePluginMetaKeeper();
    Assert.assertEquals(
        BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginClass(),
        keeper.getPluginClass(BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName()));
  }
}
