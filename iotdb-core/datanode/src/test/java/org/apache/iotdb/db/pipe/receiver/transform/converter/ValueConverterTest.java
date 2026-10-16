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

package org.apache.iotdb.db.pipe.receiver.transform.converter;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.junit.Assert;
import org.junit.Test;

public class ValueConverterTest {

  @Test
  public void testTextLikeDateTimeToDate() {
    final int expectedDate = DateUtils.parseDateExpressionToInt("2024-06-28");
    final Binary dateTime = binary("2024-06-28 08:00:00");

    Assert.assertEquals(expectedDate, ValueConverter.convertTextToDate(dateTime));
    Assert.assertEquals(expectedDate, ValueConverter.convertStringToDate(dateTime));
    Assert.assertEquals(expectedDate, ValueConverter.convertBlobToDate(dateTime));
  }

  @Test
  public void testTextLikeDateToDateKeepsExistingFallbacks() {
    final int expectedDate = DateUtils.parseDateExpressionToInt("2024-06-28");
    final int defaultDate = DateUtils.parseDateExpressionToInt("1970-01-01");

    Assert.assertEquals(expectedDate, ValueConverter.convertTextToDate(binary("20240628")));
    Assert.assertEquals(expectedDate, ValueConverter.convertTextToDate(binary("2024-06-28")));
    Assert.assertEquals(defaultDate, ValueConverter.convertTextToDate(binary("12345678910")));
    Assert.assertEquals(defaultDate, ValueConverter.convertTextToDate(binary("invalid-date")));
  }

  @Test
  public void testTextArrayDateTimeToDate() {
    final int expectedDate = DateUtils.parseDateExpressionToInt("2024-06-28");
    final int defaultDate = DateUtils.parseDateExpressionToInt("1970-01-01");

    final int[] dates =
        (int[])
            ArrayConverter.convert(
                TSDataType.TEXT,
                TSDataType.DATE,
                new Binary[] {binary("2024-06-28 08:00:00"), binary("invalid-date")});

    Assert.assertArrayEquals(new int[] {expectedDate, defaultDate}, dates);
  }

  private static Binary binary(final String value) {
    return new Binary(value, TSFileConfig.STRING_CHARSET);
  }
}
