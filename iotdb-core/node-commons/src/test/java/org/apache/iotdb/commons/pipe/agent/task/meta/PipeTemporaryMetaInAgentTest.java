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

package org.apache.iotdb.commons.pipe.agent.task.meta;

import org.junit.Assert;
import org.junit.Test;

public class PipeTemporaryMetaInAgentTest {

  @Test
  public void equalsHashCodeUsesFloatingMemoryValue() {
    PipeTemporaryMetaInAgent meta = new PipeTemporaryMetaInAgent("pipe", 1);
    meta.addFloatingMemoryUsageInByte(100);
    meta.getCommitterKey("pipe", 1, 1, 0);

    PipeTemporaryMetaInAgent sameMeta = new PipeTemporaryMetaInAgent("pipe", 1);
    sameMeta.addFloatingMemoryUsageInByte(100);
    sameMeta.getCommitterKey("pipe", 1, 1, 0);

    Assert.assertEquals(meta, sameMeta);
    Assert.assertEquals(meta.hashCode(), sameMeta.hashCode());

    sameMeta.decreaseFloatingMemoryUsageInByte(1);
    Assert.assertNotEquals(meta, sameMeta);
  }
}
