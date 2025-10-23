/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.source;

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.db.pipe.source.dataregion.IoTDBDataRegionSource;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class IoTDBDataRegionSourceTest {
  @Test
  public void testIoTDBDataRegionExtractor() {
    try (final IoTDBDataRegionSource extractor = new IoTDBDataRegionSource()) {
      extractor.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(PipeSourceConstant.EXTRACTOR_HISTORY_ENABLE_KEY, Boolean.TRUE.toString());
                      put(
                          PipeSourceConstant.EXTRACTOR_REALTIME_ENABLE_KEY,
                          Boolean.TRUE.toString());
                      put(
                          PipeSourceConstant.EXTRACTOR_REALTIME_MODE_KEY,
                          PipeSourceConstant.EXTRACTOR_REALTIME_MODE_HYBRID_VALUE);
                    }
                  })));
    } catch (final Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testIoTDBDataRegionExtractorWithPattern() {
    Assert.assertEquals(
        IllegalArgumentException.class,
        testIoTDBDataRegionExtractorWithPattern("root.a-b").getClass());
    Assert.assertEquals(
        IllegalArgumentException.class,
        testIoTDBDataRegionExtractorWithPattern("root.1.a").getClass());
    Assert.assertEquals(
        IllegalArgumentException.class, testIoTDBDataRegionExtractorWithPattern("r").getClass());
    Assert.assertEquals(
        IllegalArgumentException.class, testIoTDBDataRegionExtractorWithPattern("").getClass());
    Assert.assertEquals(
        IllegalArgumentException.class, testIoTDBDataRegionExtractorWithPattern("123").getClass());
    Assert.assertEquals(
        IllegalArgumentException.class,
        testIoTDBDataRegionExtractorWithPattern("root.a b").getClass());
    Assert.assertEquals(
        IllegalArgumentException.class,
        testIoTDBDataRegionExtractorWithPattern("root.a+b").getClass());
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.ab."));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.a#b"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.一二三"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.一二。三"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root."));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.ab"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.a.b.c.1e2"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.`a-b`"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.1"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.a,root.b"));
    Assert.assertNull(testIoTDBDataRegionExtractorWithPattern("root.a,root.b,root.db1.`a,b`.**"));
  }

  public Exception testIoTDBDataRegionExtractorWithPattern(final String pattern) {
    try (final IoTDBDataRegionSource extractor = new IoTDBDataRegionSource()) {
      extractor.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, pattern);
                    }
                  })));
    } catch (final Exception e) {
      return e;
    }
    return null;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIoTDBDataRegionExtractorWithDeletionAndPattern() throws Exception {
    try (final IoTDBDataRegionSource extractor = new IoTDBDataRegionSource()) {
      extractor.validate(
          new PipeParameterValidator(
              new PipeParameters(
                  new HashMap<String, String>() {
                    {
                      put(PipeSourceConstant.EXTRACTOR_PATTERN_KEY, "root.db");
                      put(PipeSourceConstant.EXTRACTOR_INCLUSION_KEY, "data.delete");
                    }
                  })));
    }
  }
}
