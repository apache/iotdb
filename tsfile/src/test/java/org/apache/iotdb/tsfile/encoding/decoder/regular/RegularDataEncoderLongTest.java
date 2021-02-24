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
package org.apache.iotdb.tsfile.encoding.decoder.regular;

import org.apache.iotdb.tsfile.encoding.decoder.RegularDataDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.RegularDataEncoder;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class RegularDataEncoderLongTest {

  private static int ROW_NUM;
  ByteArrayOutputStream out;
  private RegularDataEncoder regularDataEncoder;
  private RegularDataDecoder regularDataDecoder;
  private ByteBuffer buffer;

  @Before
  public void test() {
    regularDataEncoder = new RegularDataEncoder.LongRegularEncoder();
    regularDataDecoder = new RegularDataDecoder.LongRegularDecoder();
  }

  @Test
  public void testRegularEncodingWithoutMissingPoint() throws IOException {
    List<String> dates = getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00");

    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    ROW_NUM = dates.size();

    long[] data = new long[ROW_NUM];
    for (int i = 0; i < dates.size(); i++) {
      try {
        Date date = dateFormat.parse(dates.get(i));
        data[i] = date.getTime();
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithOnePercentMissingPoints1() throws IOException {
    long[] data =
        getMissingPointData(
            getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 80);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithOnePercentMissingPoints2() throws IOException {
    long[] data =
        getMissingPointData(
            getBetweenDateWithTwoSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 80);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithFivePercentMissingPoints() throws IOException {
    long[] data =
        getMissingPointData(
            getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 20);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithTenPercentMissingPoints() throws IOException {
    long[] data =
        getMissingPointData(
            getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 10);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithTwentyPercentMissingPoints() throws IOException {
    long[] data =
        getMissingPointData(
            getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 5);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithLowMissingPoints1() throws IOException {
    long[] data =
        getMissingPointData(
            getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 1700);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithLowMissingPoints2() throws IOException {
    long[] data =
        getMissingPointData(
            getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 40000);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testMissingPointsDataSize() throws IOException {
    long[] originalData = new long[] {1000, 1100, 1200, 1300, 1500, 2000};
    out = new ByteArrayOutputStream();
    writeData(originalData, 6);
    byte[] page = out.toByteArray();
    buffer = ByteBuffer.wrap(page);
    int i = 0;
    while (regularDataDecoder.hasNext(buffer)) {
      assertEquals(originalData[i++], regularDataDecoder.readLong(buffer));
    }
  }

  private long[] getMissingPointData(List<String> originalData, int missingPointInterval) {
    List<String> dates = originalData;

    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    int kong = 0;
    for (int i = 0; i < dates.size(); i++) {
      if (i % missingPointInterval == 0) {
        kong++;
      }
    }

    ROW_NUM = dates.size() - kong;

    long[] data = new long[ROW_NUM];
    int j = 0;
    for (int i = 0; i < dates.size(); i++) {
      if (i % missingPointInterval == 0) {
        continue;
      }

      try {
        Date date = dateFormat.parse(dates.get(i));
        data[j++] = date.getTime();
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }

    return data;
  }

  private List<String> getBetweenDateWithOneSecond(String start, String end) {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    List<String> list = new ArrayList<>();
    LocalDateTime startDate = LocalDateTime.parse(start);
    LocalDateTime endDate = LocalDateTime.parse(end);

    long distance = ChronoUnit.SECONDS.between(startDate, endDate);
    if (distance < 1) {
      return list;
    }
    Stream.iterate(
            startDate,
            d -> {
              return d.plusSeconds(1);
            })
        .limit(distance + 1)
        .forEach(
            f -> {
              list.add(f.format(formatter));
            });
    return list;
  }

  private List<String> getBetweenDateWithTwoSecond(String start, String end) {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    List<String> list = new ArrayList<>();
    LocalDateTime startDate = LocalDateTime.parse(start);
    LocalDateTime endDate = LocalDateTime.parse(end);

    long distance = ChronoUnit.SECONDS.between(startDate, endDate);
    if (distance < 1) {
      return list;
    }
    Stream.iterate(
            startDate,
            d -> {
              return d.plusSeconds(2);
            })
        .limit((distance / 2) + 1)
        .forEach(
            f -> {
              list.add(f.format(formatter));
            });
    return list;
  }

  private void writeData(long[] data, int length) {
    for (int i = 0; i < length; i++) {
      regularDataEncoder.encode(data[i], out);
    }
    regularDataEncoder.flush(out);
  }

  private void shouldReadAndWrite(long[] data, int length) throws IOException {
    out = new ByteArrayOutputStream();
    writeData(data, length);
    byte[] page = out.toByteArray();
    buffer = ByteBuffer.wrap(page);
    int i = 0;
    while (regularDataDecoder.hasNext(buffer)) {
      assertEquals(data[i++], regularDataDecoder.readLong(buffer));
    }
  }
}
