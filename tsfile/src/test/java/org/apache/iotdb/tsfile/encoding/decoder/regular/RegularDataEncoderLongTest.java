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

import static org.junit.Assert.assertEquals;

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

import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.junit.Before;
import org.junit.Test;

import org.apache.iotdb.tsfile.encoding.decoder.RegularDataDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.RegularDataEncoder;

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
    long[] data = getMissingPointData(getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 80);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithOnePercentMissingPoints2() throws IOException {
    long[] data = getMissingPointData(getBetweenDateWithTwoSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 80);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithFivePercentMissingPoints() throws IOException {
    long[] data = getMissingPointData(getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 20);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithTenPercentMissingPoints() throws IOException {
    long[] data = getMissingPointData(getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 10);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithTwentyPercentMissingPoints() throws IOException {
    long[] data = getMissingPointData(getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 5);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithLowMissingPoints1() throws IOException {
    long[] data = getMissingPointData(getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 1700);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithLowMissingPoints2() throws IOException {
    long[] data = getMissingPointData(getBetweenDateWithOneSecond("1980-01-01T01:00:00", "1980-01-28T01:00:00"), 40000);

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test(expected = TsFileEncodingException.class)
  public void testRegularWithMissingPointsThrowException() throws IOException {
    long[] data = new long[] {1200, 1100, 1000, 2200};

    shouldReadAndWrite(data, 4);
  }

  @Test
  public void testMissingPointsNoRegularDesc() throws IOException {
    long[] data = new long[] {1000, 900, 800, 700, 600, 0};

    shouldReadAndWrite(data, 6);
  }

  @Test
  public void testMissingPointsNoRegularDescRevise() throws IOException {
    long[] originalData = new long[] {1000, 900, 800, 700, 1200, 0};
    long[] correctData = new long[] {1000, 900, 800, 700, 600, 0};
    out = new ByteArrayOutputStream();
    writeData(originalData, 6);
    byte[] page = out.toByteArray();
    System.out.println("encoding data size:" + page.length + " byte");
    buffer = ByteBuffer.wrap(page);
    int i = 0;
    while (regularDataDecoder.hasNext(buffer)) {
      assertEquals(correctData[i++], regularDataDecoder.readInt(buffer));
    }
  }

  @Test
  public void testMissingPointsNoRegularCorrect() throws IOException {
    long[] originalData = new long[] {1000, 1100, 1200, 1500, 1700, 1500};
    long[] correctData = new long[] {1000, 1100, 1200, 1300, 1400, 1500};
    out = new ByteArrayOutputStream();
    writeData(originalData, 6);
    byte[] page = out.toByteArray();
    System.out.println("encoding data size:" + page.length + " byte");
    buffer = ByteBuffer.wrap(page);
    int i = 0;
    while (regularDataDecoder.hasNext(buffer)) {
      assertEquals(correctData[i++], regularDataDecoder.readInt(buffer));
    }
  }

  @Test
  public void testMissingPointsRegular() throws IOException {
    long[] originalData = new long[] {1000, 1100, 1200, 1800, 1900};
    shouldReadAndWrite(originalData, 5);
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
    Stream.iterate(startDate, d -> {
      return d.plusSeconds(1);
    }).limit(distance + 1).forEach(f -> {
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
    Stream.iterate(startDate, d -> {
      return d.plusSeconds(2);
    }).limit((distance / 2) + 1).forEach(f -> {
      list.add(f.format(formatter));
    });
    return list;
  }

  private void writeData(long[] data, int length) throws IOException {
    for (int i = 0; i < length; i++) {
      regularDataEncoder.encode(data[i], out);
    }
    regularDataEncoder.flush(out);
  }

  private void shouldReadAndWrite(long[] data, int length) throws IOException {
    System.out.println("source data size:" + 8 * length + " byte");
    out = new ByteArrayOutputStream();
    writeData(data, length);
    byte[] page = out.toByteArray();
    System.out.println("encoding data size:" + page.length + " byte");
    buffer = ByteBuffer.wrap(page);
    int i = 0;
    while (regularDataDecoder.hasNext(buffer)) {
      assertEquals(data[i++], regularDataDecoder.readLong(buffer));
    }
  }
}
