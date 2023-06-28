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

package org.apache.iotdb.tsfile.exception;

import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.exception.compress.CompressionTypeNotSupportedException;
import org.apache.iotdb.tsfile.exception.compress.GZIPCompressOverflowException;
import org.apache.iotdb.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.filter.StatisticsClassException;
import org.apache.iotdb.tsfile.exception.filter.UnSupportFilterDataTypeException;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TsFileExceptionTest {

  private static final String MOCK = "mock";

  @Test
  public void testNotCompatibleTsFileException() {
    NotCompatibleTsFileException e = new NotCompatibleTsFileException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  @Test
  public void testNotImplementedException() {
    NotImplementedException e = new NotImplementedException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  @Test
  public void testNullFieldException() {
    NullFieldException e = new NullFieldException();
    assertEquals("Field is null", e.getMessage());
  }

  @Test
  public void testPathParseException() {
    PathParseException e = new PathParseException(MOCK);
    assertEquals("mock is not a legal path.", e.getMessage());
  }

  @Test
  public void testTsFileRuntimeException() {
    TsFileRuntimeException e = new TsFileRuntimeException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  @Test
  public void testTsFileStatisticsMistakesException() {
    TsFileStatisticsMistakesException e = new TsFileStatisticsMistakesException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  // test for cache exception
  @Test
  public void testCacheException() {
    CacheException e = new CacheException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  // test for compress exception
  @Test
  public void testCompressionTypeNotSupportedException() {
    CompressionTypeNotSupportedException e = new CompressionTypeNotSupportedException(MOCK);
    assertEquals("codec not supported: " + MOCK, e.getMessage());
  }

  @Test
  public void testGZIPCompressOverflowException() {
    GZIPCompressOverflowException e = new GZIPCompressOverflowException();
    assertEquals("compressed data is larger than the given byte container.", e.getMessage());
  }

  // test for encoding exception
  @Test
  public void testTsFileDecodingException() {
    TsFileDecodingException e = new TsFileDecodingException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  @Test
  public void testTsFileEncodingException() {
    TsFileEncodingException e = new TsFileEncodingException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  // test for filter exception
  @Test
  public void testQueryFilterOptimizationException() {
    QueryFilterOptimizationException e = new QueryFilterOptimizationException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  @Test
  public void testStatisticsClassException() {
    StatisticsClassException e = new StatisticsClassException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  @Test
  public void testUnSupportFilterDataTypeException() {
    UnSupportFilterDataTypeException e = new UnSupportFilterDataTypeException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  // test for write exception
  @Test
  public void testNoMeasurementException() {
    NoMeasurementException e = new NoMeasurementException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  @Test
  public void testPageException() {
    PageException e = new PageException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  @Test
  public void testTsFileNotCompleteException() {
    TsFileNotCompleteException e = new TsFileNotCompleteException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }

  @Test
  public void testUnknownColumnTypeException() {
    UnknownColumnTypeException e = new UnknownColumnTypeException(MOCK);
    assertEquals("Column type not found: " + MOCK, e.getMessage());
  }

  @Test
  public void testUnSupportedDataTypeException() {
    UnSupportedDataTypeException e = new UnSupportedDataTypeException(MOCK);
    assertEquals("Unsupported dataType: " + MOCK, e.getMessage());
  }

  @Test
  public void testWriteProcessException() {
    WriteProcessException e = new WriteProcessException(MOCK);
    assertEquals(MOCK, e.getMessage());
  }
}
