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

package org.apache.iotdb.commons.utils;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.iotdb.commons.utils.CommonDateTimeUtils.convertMillisecondToDurationStr;

public class CommonDateTimeUtilsTest {
  @Test
  public void convertMillisecondToDurationStrTest() {
    Assert.assertEquals("0 ms", convertMillisecondToDurationStr(0));
    Assert.assertEquals("1 ms", convertMillisecondToDurationStr(1));
    Assert.assertEquals("999 ms", convertMillisecondToDurationStr(999));
    Assert.assertEquals(
        "1 day 3 hour 46 minute 39 second 999 ms", convertMillisecondToDurationStr(99999999));
    Assert.assertEquals("1 day 3 hour 39 second 999 ms", convertMillisecondToDurationStr(97239999));
    Assert.assertEquals(
        "3170979 year 2 month 12 day 9 hour 46 minute 39 second 999 ms",
        convertMillisecondToDurationStr(99999999999999999L));
    Assert.assertEquals("-(10 second 86 ms)", convertMillisecondToDurationStr(-10086));
  }
}
