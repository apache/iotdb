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

package org.apache.iotdb.tsfile.encoding.elf;

import org.junit.Assert;
import org.junit.Test;

public class ElfDoublePrecisionBenchCodecTest {

  @Test
  public void roundTripSmallSeries() {
    double[] values = {0.0, 1.25, -3.5, 42.125, 1e-6, 100.0, 200.5};
    byte[] encoded = ElfDoublePrecisionBenchCodec.encode(values);
    Assert.assertTrue(encoded.length > 0);
    double[] decoded = ElfDoublePrecisionBenchCodec.decode(encoded);
    Assert.assertEquals(values.length, decoded.length);
    for (int i = 0; i < values.length; i++) {
      if (Double.isNaN(values[i])) {
        Assert.assertTrue(Double.isNaN(decoded[i]));
      } else {
        Assert.assertEquals(values[i], decoded[i], 0.0);
      }
    }
  }
}
