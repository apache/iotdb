package org.apache.iotdb.commons.sync;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TsFilePipeInfoTest {
  @Test
  public void testTsFilePipeInfo() throws IOException {
    PipeInfo pipeInfo = new TsFilePipeInfo("name", "demo", System.currentTimeMillis(), 999, false);
    Assert.assertEquals(PipeStatus.STOP, pipeInfo.status);
  }
}
