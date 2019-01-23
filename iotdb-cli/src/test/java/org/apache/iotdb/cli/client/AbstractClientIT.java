/**
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
package org.apache.iotdb.cli.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.iotdb.cli.client.AbstractClient.OperationResult;
import org.apache.iotdb.cli.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBDatabaseMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AbstractClientIT {

  @Mock
  private IoTDBConnection connection;

  @Mock
  private IoTDBDatabaseMetadata databaseMetadata;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(connection.getMetaData()).thenReturn(databaseMetadata);
    when(connection.getTimeZone()).thenReturn("Asia/Shanghai");
    when(databaseMetadata.getMetadataInJson()).thenReturn("test metadata");
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testInit() {
    AbstractClient.init();
    String[] keywords = {AbstractClient.HOST_ARGS, AbstractClient.HELP_ARGS,
        AbstractClient.PORT_ARGS,
        AbstractClient.PASSWORD_ARGS, AbstractClient.USERNAME_ARGS, AbstractClient.ISO8601_ARGS,
        AbstractClient.MAX_PRINT_ROW_COUNT_ARGS,};
    for (String keyword : keywords) {
      if (!AbstractClient.keywordSet.contains("-" + keyword)) {
        System.out.println(keyword);
        fail();
      }
    }
  }

  @Test
  public void testCheckRequiredArg() throws ParseException, ArgsErrorException {
    Options options = AbstractClient.createOptions();
    CommandLineParser parser = new DefaultParser();
    String[] args = new String[]{"-u", "user1"};
    CommandLine commandLine = parser.parse(options, args);
    String str = AbstractClient
        .checkRequiredArg(AbstractClient.USERNAME_ARGS, AbstractClient.USERNAME_NAME,
            commandLine, true, "root");
    assertEquals("user1", str);

    args = new String[]{"-u", "root",};
    commandLine = parser.parse(options, args);
    str = AbstractClient
        .checkRequiredArg(AbstractClient.HOST_ARGS, AbstractClient.HOST_NAME, commandLine, false,
            "127.0.0.1");
    assertEquals("127.0.0.1", str);
    try {
      str = AbstractClient
          .checkRequiredArg(AbstractClient.HOST_ARGS, AbstractClient.HOST_NAME, commandLine, true,
              "127.0.0.1");
    } catch (ArgsErrorException e) {
      assertEquals("IoTDB: Required values for option 'host' not provided", e.getMessage());
    }
    try {
      str = AbstractClient
          .checkRequiredArg(AbstractClient.HOST_ARGS, AbstractClient.HOST_NAME, commandLine,
              false, null);
    } catch (ArgsErrorException e) {
      assertEquals("IoTDB: Required values for option 'host' is null.", e.getMessage());
    }
  }

  @Test
  public void testRemovePasswordArgs() {
    AbstractClient.init();
    String[] input = new String[]{"-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root"};
    String[] res = new String[]{"-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root"};
    isTwoStringArrayEqual(res, AbstractClient.removePasswordArgs(input));

    input = new String[]{"-h", "127.0.0.1", "-p", "6667", "-pw", "root", "-u", "root"};
    res = new String[]{"-h", "127.0.0.1", "-p", "6667", "-pw", "root", "-u", "root"};
    isTwoStringArrayEqual(res, AbstractClient.removePasswordArgs(input));

    input = new String[]{"-h", "127.0.0.1", "-p", "6667", "root", "-u", "root", "-pw"};
    res = new String[]{"-h", "127.0.0.1", "-p", "6667", "root", "-u", "root"};
    isTwoStringArrayEqual(res, AbstractClient.removePasswordArgs(input));

    input = new String[]{"-h", "127.0.0.1", "-p", "6667", "-pw", "-u", "root"};
    res = new String[]{"-h", "127.0.0.1", "-p", "6667", "-u", "root"};
    isTwoStringArrayEqual(res, AbstractClient.removePasswordArgs(input));

    input = new String[]{"-pw", "-h", "127.0.0.1", "-p", "6667", "root", "-u", "root"};
    res = new String[]{"-h", "127.0.0.1", "-p", "6667", "root", "-u", "root"};
    isTwoStringArrayEqual(res, AbstractClient.removePasswordArgs(input));

    input = new String[]{};
    res = new String[]{};
    isTwoStringArrayEqual(res, AbstractClient.removePasswordArgs(input));
  }

  private void isTwoStringArrayEqual(String[] expected, String[] actual) {
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual[i]);
    }
  }

  @Test
  public void testHandleInputInputCmd() {
    assertEquals(OperationResult.RETURN_OPER, AbstractClient.handleInputCmd(AbstractClient.EXIT_COMMAND, connection));
    assertEquals(OperationResult.RETURN_OPER, AbstractClient.handleInputCmd(AbstractClient.QUIT_COMMAND, connection));

    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(AbstractClient.SHOW_METADATA_COMMAND, connection));

    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(String.format("%s=", AbstractClient.SET_TIMESTAMP_DISPLAY), connection));
    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(String.format("%s=xxx", AbstractClient.SET_TIMESTAMP_DISPLAY), connection));
    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(String.format("%s=default", AbstractClient.SET_TIMESTAMP_DISPLAY), connection));
    testSetTimeFormat();

    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(String.format("%s=", AbstractClient.SET_MAX_DISPLAY_NUM), connection));
    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(String.format("%s=xxx", AbstractClient.SET_MAX_DISPLAY_NUM),connection));
    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(String.format("%s=1", AbstractClient.SET_MAX_DISPLAY_NUM), connection));
    testSetMaxDisplayNumber();

    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(AbstractClient.SHOW_TIMEZONE, connection));
    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(AbstractClient.SHOW_TIMESTAMP_DISPLAY, connection));
    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(AbstractClient.SHOW_FETCH_SIZE, connection));

    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(String.format("%s=", AbstractClient.SET_TIME_ZONE), connection));
    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(String.format("%s=+08:00", AbstractClient.SET_TIME_ZONE), connection));

    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(String.format("%s=", AbstractClient.SET_FETCH_SIZE), connection));
    assertEquals(OperationResult.CONTINUE_OPER, AbstractClient.handleInputCmd(String.format("%s=111", AbstractClient.SET_FETCH_SIZE), connection));
  }

  private void testSetTimeFormat() {
    AbstractClient.setTimeFormat("long");
    assertEquals(AbstractClient.maxTimeLength, AbstractClient.maxValueLength);
    assertEquals(AbstractClient.formatTime, "%" + AbstractClient.maxTimeLength + "s|");

    AbstractClient.setTimeFormat("number");
    assertEquals(AbstractClient.maxTimeLength, AbstractClient.maxValueLength);
    assertEquals(AbstractClient.formatTime, "%" + AbstractClient.maxTimeLength + "s|");

    AbstractClient.setTimeFormat("default");
    assertEquals(AbstractClient.ISO_DATETIME_LEN, AbstractClient.maxTimeLength);
    assertEquals(AbstractClient.formatTime, "%" + AbstractClient.maxTimeLength + "s|");

    AbstractClient.setTimeFormat("iso8601");
    assertEquals(AbstractClient.ISO_DATETIME_LEN, AbstractClient.maxTimeLength);
    assertEquals(AbstractClient.formatTime, "%" + AbstractClient.maxTimeLength + "s|");

    AbstractClient.setTimeFormat("yyyy-MM-dd HH:mm:ssZZ");
    assertEquals(AbstractClient.maxTimeLength, "yyyy-MM-dd HH:mm:ssZZ".length());
    assertEquals(AbstractClient.formatTime, "%" + AbstractClient.maxTimeLength + "s|");

    AbstractClient.setTimeFormat("dd");
    assertEquals(AbstractClient.maxTimeLength, AbstractClient.TIMESTAMP_STR.length());
    assertEquals(AbstractClient.formatTime, "%" + AbstractClient.maxTimeLength + "s|");

  }

  private void testSetMaxDisplayNumber() {
    AbstractClient.setMaxDisplayNumber("10");
    assertEquals(10, AbstractClient.maxPrintRowCount);
    AbstractClient.setMaxDisplayNumber("111111111111111");
    assertEquals(Integer.MAX_VALUE, AbstractClient.maxPrintRowCount);
    AbstractClient.setMaxDisplayNumber("-10");
    assertEquals(Integer.MAX_VALUE, AbstractClient.maxPrintRowCount);
  }
}
