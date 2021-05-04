package org.apache.iotdb.db.engine.migration.task;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.function.BiConsumer;

public class MigrationTaskTest extends MigrationTest {
  private final boolean sequence = true;
  private final int timePartitionId = 0;

  @Test
  public void testMigrate() {
    MigrationTask task =
        new MigrationTask(
            srcResources,
            targetDir,
            sequence,
            timePartitionId,
            this::migrationCallback,
            testSgName,
            testSgSysDir);
    task.run();

    for (File src : srcFiles) {
      File srcResource = fsFactory.getFile(src.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Assert.assertFalse(src.exists());
      Assert.assertFalse(srcResource.exists());

      File target = fsFactory.getFile(testSgTier2Dir, src.getName());
      File targetResource = fsFactory.getFile(target.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Assert.assertTrue(target.exists());
      Assert.assertTrue(targetResource.exists());
    }
  }

  private void migrationCallback(
      File tsFileToDelete,
      File tsFileToLoad,
      boolean sequence,
      BiConsumer<File, File> opsToBothFiles) {
    Assert.assertTrue(tsFileToDelete.exists());
    Assert.assertFalse(tsFileToLoad.exists());

    if (opsToBothFiles != null) {
      opsToBothFiles.accept(tsFileToDelete, tsFileToLoad);
    }

    Assert.assertFalse(tsFileToDelete.exists());
    Assert.assertTrue(tsFileToLoad.exists());
  }
}
