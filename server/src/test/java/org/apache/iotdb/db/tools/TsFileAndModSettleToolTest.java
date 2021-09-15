package org.apache.iotdb.db.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;
import org.apache.iotdb.db.utils.TsFileRewriteToolTest;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.junit.Assert;
import org.junit.Test;

public class TsFileAndModSettleToolTest extends TsFileRewriteToolTest {

  public TsFileAndModSettleToolTest() throws QueryProcessException {
  }

  @Test
  public void settleTsFilesAndModsTest() {
    HashMap<String, List<String>> deviceSensorsMap = new HashMap<>();
    List<String> sensors = new ArrayList<>();
    sensors.add(SENSOR1);
    deviceSensorsMap.put(DEVICE1, sensors);
    createOneTsFile(deviceSensorsMap);

    String timeseriesPath = STORAGE_GROUP + DEVICE1 + SENSOR1;
    createlModificationFile(timeseriesPath);

    File tsFile = new File("");
    try {
      TsFileAndModSettleTool.settleOneTsFileAndMod(new TsFileResource(tsFile));
    } catch (WriteProcessException | IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
