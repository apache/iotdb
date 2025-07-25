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

package org.apache.iotdb.confignode.manager.pipe.source;

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class IoTDBConfigRegionSourceTest {
  @Test
  public void testIoTDBConfigExtractor() {
    try (final IoTDBConfigRegionSource extractor = new IoTDBConfigRegionSource()) {
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
                      put(
                          PipeSourceConstant.EXTRACTOR_INCLUSION_KEY,
                          PipeSourceConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE);
                    }
                  })));
    } catch (final Exception e) {
      Assert.fail();
    }
  }
}
