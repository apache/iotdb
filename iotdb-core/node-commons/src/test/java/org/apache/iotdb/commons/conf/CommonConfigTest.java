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

package org.apache.iotdb.commons.conf;

import org.junit.Assert;
import org.junit.Test;

public class CommonConfigTest {

  @Test
  public void testAutoResizingBufferMemoryProportionCanDisableMemoryControl() {
    CommonConfig config = new CommonConfig();

    config.setAutoResizingBufferMemoryProportion(0);
    Assert.assertEquals(0, config.getAutoResizingBufferMemoryProportion(), 0);

    config.setAutoResizingBufferMemoryProportion(-1);
    Assert.assertEquals(-1, config.getAutoResizingBufferMemoryProportion(), 0);
  }

  @Test
  public void testInvalidAutoResizingBufferMemoryProportionIsIgnored() {
    CommonConfig config = new CommonConfig();
    double originalValue = config.getAutoResizingBufferMemoryProportion();

    config.setAutoResizingBufferMemoryProportion(1.1);
    Assert.assertEquals(originalValue, config.getAutoResizingBufferMemoryProportion(), 0);

    config.setAutoResizingBufferMemoryProportion(Double.NaN);
    Assert.assertEquals(originalValue, config.getAutoResizingBufferMemoryProportion(), 0);
  }
}
