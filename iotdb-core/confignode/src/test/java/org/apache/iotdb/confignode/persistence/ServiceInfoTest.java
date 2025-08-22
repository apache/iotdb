package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.service.external.ServiceInformation;
import org.apache.iotdb.commons.service.external.ServiceStatus;
import org.apache.iotdb.confignode.consensus.request.write.service.CreateServicePlan;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.utils.Binary;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class ServiceInfoTest {
  private static ServiceInfo serviceInfo;
  private static ServiceInfo serviceInfoSaveBefore;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    serviceInfo = new ServiceInfo();
    serviceInfoSaveBefore = new ServiceInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    serviceInfo.clear();
    if (snapshotDir.exists()) {
      // Clean up the snapshot directory after tests
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testTakeSnapshot() throws Exception {
    ServiceInformation serviceInformation =
        new ServiceInformation(
            "testService",
            "TestServiceClass",
            true,
            "testService.jar",
            "md5checksum",
            ServiceStatus.INACTIVE);
    CreateServicePlan createServicePlan =
        new CreateServicePlan(serviceInformation, new Binary(new byte[] {1, 2, 3}));
    serviceInfo.addServiceInTable(createServicePlan);
    serviceInfoSaveBefore.addServiceInTable(createServicePlan);

    // Take snapshot
    serviceInfo.processTakeSnapshot(snapshotDir);

    // Clear current state
    serviceInfo.clear();

    // Load from snapshot
    serviceInfo.processLoadSnapshot(snapshotDir);

    // Verify the loaded state matches the saved state
    Assert.assertEquals(
        serviceInfo.getRawExistedJarToMD5(), serviceInfoSaveBefore.getRawExistedJarToMD5());
    Assert.assertEquals(
        serviceInfo.getRawServiceTable(), serviceInfoSaveBefore.getRawServiceTable());
  }
}
