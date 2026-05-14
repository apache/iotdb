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

package org.apache.iotdb.confignode.consensus.statemachine;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConfigRegionStateMachineTest {

  @Test
  public void testParseStartIndex() {
    Assert.assertEquals(1, ConfigRegionStateMachine.parseStartIndex("log_1_10"));
    Assert.assertEquals(11, ConfigRegionStateMachine.parseStartIndex("log_11_20"));
    Assert.assertEquals(21, ConfigRegionStateMachine.parseStartIndex("log_inprogress_21"));
    Assert.assertEquals(0, ConfigRegionStateMachine.parseStartIndex("invalid"));
  }

  @Test
  public void testFileComparatorSortsByStartIndex() {
    List<String> filenames =
        new ArrayList<>(Arrays.asList("log_inprogress_21", "log_11_20", "log_1_10"));

    filenames.sort(new ConfigRegionStateMachine.FileComparator());

    Assert.assertEquals(Arrays.asList("log_1_10", "log_11_20", "log_inprogress_21"), filenames);
  }
}
