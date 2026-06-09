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

package org.apache.iotdb.db.pipe.processor.twostage.plugin;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TwoStageCountProcessorTest {

  @Test
  public void testOutputSeriesSupportsNewAndLegacyKeys() throws Exception {
    Assert.assertEquals(
        "root.db.d.s1", parseOutputSeries("processor.output.series", "root.db.d.s1").getFullPath());
    Assert.assertEquals(
        "root.db.d.s2", parseOutputSeries("processor.output-series", "root.db.d.s2").getFullPath());
  }

  @Test
  public void testValidateOutputSeriesSupportsNewAndLegacyKeys() throws Exception {
    validateOutputSeries("processor.output.series", "root.db.d.s1");
    validateOutputSeries("processor.output-series", "root.db.d.s2");
  }

  private PartialPath parseOutputSeries(final String key, final String value) throws Exception {
    return TwoStageCountProcessor.parseOutputSeries(
        new PipeParameters(Collections.singletonMap(key, value)));
  }

  private void validateOutputSeries(final String key, final String value) throws Exception {
    new TwoStageCountProcessor()
        .validate(
            new PipeParameterValidator(new PipeParameters(Collections.singletonMap(key, value))));
  }
}
