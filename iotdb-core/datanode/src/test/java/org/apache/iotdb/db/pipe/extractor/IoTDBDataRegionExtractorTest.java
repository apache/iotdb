package org.apache.iotdb.db.pipe.extractor;

import org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class IoTDBDataRegionExtractorTest {
  @Test
  public void testIoTDBDataRegionExtractorTest() {
    IoTDBDataRegionExtractor extractor = new IoTDBDataRegionExtractor();
    try {
      extractor.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(
                          PipeExtractorConstant.EXTRACTOR_HISTORY_ENABLE_KEY,
                          Boolean.TRUE.toString());
                      put(PipeExtractorConstant.EXTRACTOR_REALTIME_ENABLE, Boolean.TRUE.toString());
                      put(
                          PipeExtractorConstant.EXTRACTOR_REALTIME_MODE,
                          PipeExtractorConstant.EXTRACTOR_REALTIME_MODE_HYBRID);
                    }
                  })));
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
