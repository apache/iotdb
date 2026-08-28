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

package org.apache.iotdb.cli;

import org.apache.iotdb.cli.AbstractCli.OperationResult;
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBDatabaseMetadata;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class AbstractCliTest {
  private static Logger logger = LoggerFactory.getLogger(AbstractCliTest.class);
  @Mock private IoTDBConnection connection;

  @Mock private IoTDBDatabaseMetadata databaseMetadata;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(connection.getMetaData()).thenReturn(databaseMetadata);
    when(connection.getTimeZone()).thenReturn("Asia/Shanghai");
  }

  @After
  public void tearDown() throws Exception {
    setStaticField("lineCount", 0);
    setStaticField("isReachEnd", false);
  }

  @Test
  public void testInit() {
    AbstractCli.init();
    String[] keywords = {
      AbstractCli.HOST_ARGS,
      AbstractCli.HELP_ARGS,
      AbstractCli.PORT_ARGS,
      AbstractCli.PW_ARGS,
      AbstractCli.USERNAME_ARGS,
      AbstractCli.ISO8601_ARGS,
    };
    for (String keyword : keywords) {
      if (!AbstractCli.keywordSet.contains("-" + keyword)) {
        logger.error(keyword);
        fail();
      }
    }
  }

  @Test
  public void testCheckRequiredArg() throws ParseException, ArgsErrorException {
    CliContext ctx = new CliContext(System.in, System.out, System.err, ExitType.EXCEPTION);
    Options options = AbstractCli.createOptions();
    CommandLineParser parser = new DefaultParser();
    String[] args = new String[] {"-u", "user1"};
    CommandLine commandLine = parser.parse(options, args);
    String str =
        AbstractCli.checkRequiredArg(
            ctx, AbstractCli.USERNAME_ARGS, AbstractCli.USERNAME_NAME, commandLine, true, "root");
    assertEquals("user1", str);

    args =
        new String[] {
          "-u", "root",
        };
    commandLine = parser.parse(options, args);
    str =
        AbstractCli.checkRequiredArg(
            ctx, AbstractCli.HOST_ARGS, AbstractCli.HOST_NAME, commandLine, false, "127.0.0.1");
    assertEquals("127.0.0.1", str);
    try {
      str =
          AbstractCli.checkRequiredArg(
              ctx, AbstractCli.HOST_ARGS, AbstractCli.HOST_NAME, commandLine, true, "127.0.0.1");
    } catch (ArgsErrorException e) {
      assertEquals("IoTDB: Required values for option 'host' not provided", e.getMessage());
    }
    try {
      str =
          AbstractCli.checkRequiredArg(
              ctx, AbstractCli.HOST_ARGS, AbstractCli.HOST_NAME, commandLine, false, null);
    } catch (ArgsErrorException e) {
      assertEquals("IoTDB: Required values for option 'host' is null.", e.getMessage());
    }
  }

  @Test
  public void testRemovePasswordArgs() {
    AbstractCli.init();
    String[] input = new String[] {"-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root"};
    String[] res = new String[] {"-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root"};
    isTwoStringArrayEqual(res, AbstractCli.removePasswordArgs(input));

    input = new String[] {"-h", "127.0.0.1", "-p", "6667", "-pw", "root", "-u", "root"};
    res = new String[] {"-h", "127.0.0.1", "-p", "6667", "-pw", "root", "-u", "root"};
    isTwoStringArrayEqual(res, AbstractCli.removePasswordArgs(input));

    input = new String[] {"-h", "127.0.0.1", "-p", "6667", "root", "-u", "root", "-pw"};
    res = new String[] {"-h", "127.0.0.1", "-p", "6667", "root", "-u", "root"};
    isTwoStringArrayEqual(res, AbstractCli.removePasswordArgs(input));

    input = new String[] {"-h", "127.0.0.1", "-p", "6667", "-pw", "-u", "root"};
    res = new String[] {"-h", "127.0.0.1", "-p", "6667", "-u", "root"};
    isTwoStringArrayEqual(res, AbstractCli.removePasswordArgs(input));

    input = new String[] {"-pw", "-h", "127.0.0.1", "-p", "6667", "root", "-u", "root"};
    res = new String[] {"-h", "127.0.0.1", "-p", "6667", "root", "-u", "root"};
    isTwoStringArrayEqual(res, AbstractCli.removePasswordArgs(input));

    input = new String[] {};
    res = new String[] {};
    isTwoStringArrayEqual(res, AbstractCli.removePasswordArgs(input));
  }

  private void isTwoStringArrayEqual(String[] expected, String[] actual) {
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual[i]);
    }
  }

  @Test
  public void testHandleInputInputCmd() {
    CliContext ctx = new CliContext(System.in, System.out, System.err, ExitType.EXCEPTION);
    assertEquals(
        OperationResult.STOP_OPER,
        AbstractCli.handleInputCmd(ctx, AbstractCli.EXIT_COMMAND, connection));
    assertEquals(
        OperationResult.STOP_OPER,
        AbstractCli.handleInputCmd(ctx, AbstractCli.QUIT_COMMAND, connection));
    assertEquals(
        OperationResult.CONTINUE_OPER,
        AbstractCli.handleInputCmd(
            ctx, String.format("%s=", AbstractCli.SET_TIMESTAMP_DISPLAY), connection));
    assertEquals(
        OperationResult.CONTINUE_OPER,
        AbstractCli.handleInputCmd(
            ctx, String.format("%s=xxx", AbstractCli.SET_TIMESTAMP_DISPLAY), connection));
    assertEquals(
        OperationResult.CONTINUE_OPER,
        AbstractCli.handleInputCmd(
            ctx, String.format("%s=default", AbstractCli.SET_TIMESTAMP_DISPLAY), connection));

    assertEquals(
        OperationResult.CONTINUE_OPER,
        AbstractCli.handleInputCmd(ctx, AbstractCli.SHOW_TIMEZONE, connection));
    assertEquals(
        OperationResult.CONTINUE_OPER,
        AbstractCli.handleInputCmd(ctx, AbstractCli.SHOW_TIMESTAMP_DISPLAY, connection));
    assertEquals(
        OperationResult.CONTINUE_OPER,
        AbstractCli.handleInputCmd(ctx, AbstractCli.SHOW_FETCH_SIZE, connection));

    assertEquals(
        OperationResult.CONTINUE_OPER,
        AbstractCli.handleInputCmd(
            ctx, String.format("%s=%s", AbstractCli.SET_TIME_ZONE, "Asis/chongqing"), connection));
    assertEquals(
        OperationResult.CONTINUE_OPER,
        AbstractCli.handleInputCmd(
            ctx, String.format("%s=+08:00", AbstractCli.SET_TIME_ZONE), connection));

    assertEquals(
        OperationResult.CONTINUE_OPER,
        AbstractCli.handleInputCmd(
            ctx, String.format("%s=", AbstractCli.SET_FETCH_SIZE), connection));
    assertEquals(
        OperationResult.CONTINUE_OPER,
        AbstractCli.handleInputCmd(
            ctx, String.format("%s=111", AbstractCli.SET_FETCH_SIZE), connection));
  }

  @Test
  public void testJsonExplainResultDetection() throws Exception {
    assertFalse(isJsonExplainResult(Collections.emptyList()));
    assertFalse(isJsonExplainResult(Collections.singletonList(Collections.singletonList("Time"))));
    assertFalse(
        isJsonExplainResult(
            Arrays.asList(
                Arrays.asList("distribution plan", "{}"), Arrays.asList("extra", "value"))));
    assertFalse(isJsonExplainResult(column("Time", "{}")));
    assertFalse(isJsonExplainResult(column("distribution plan", "OutputNode-1")));

    assertTrue(isJsonExplainResult(column("distribution plan", "  {\"name\":\"OutputNode-1\"}")));
    assertTrue(isJsonExplainResult(column("Explain Analyze", "[{\"id\":\"fragment\"}]")));
    assertTrue(isJsonExplainResult(column("explain analyze", "\n{\"planStatistics\":{}}")));
  }

  @Test
  public void testOutputRawJsonKeepsJsonContentUnframed() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream printer = new PrintStream(out, true, StandardCharsets.UTF_8.name());
    CliContext ctx = new CliContext(System.in, printer, System.err, ExitType.EXCEPTION);
    List<List<String>> lists =
        Collections.singletonList(
            Arrays.asList("distribution plan", "{", "  \"name\": \"OutputNode-1\"", "}"));

    setStaticField("lineCount", 0);
    setStaticField("isReachEnd", true);
    outputRawJson(ctx, lists, Collections.singletonList(128));

    List<String> lines =
        Arrays.asList(new String(out.toByteArray(), StandardCharsets.UTF_8).split("\\R"));
    assertEquals("+-----------------+", lines.get(0));
    assertEquals("|distribution plan|", lines.get(1));
    assertEquals("+-----------------+", lines.get(2));
    assertEquals("{", lines.get(3));
    assertEquals("  \"name\": \"OutputNode-1\"", lines.get(4));
    assertEquals("}", lines.get(5));
    assertEquals("+-----------------+", lines.get(6));
    assertEquals("Total line number = 3", lines.get(7));
    assertFalse(lines.get(3).startsWith("|"));
  }

  private static List<List<String>> column(String header, String value) {
    return Collections.singletonList(Arrays.asList(header, value));
  }

  private static boolean isJsonExplainResult(List<List<String>> lists) throws Exception {
    Method method = AbstractCli.class.getDeclaredMethod("isJsonExplainResult", List.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, lists);
  }

  private static void outputRawJson(
      CliContext ctx, List<List<String>> lists, List<Integer> maxSizeList) throws Exception {
    Method method =
        AbstractCli.class.getDeclaredMethod(
            "outputRawJson", CliContext.class, List.class, List.class);
    method.setAccessible(true);
    method.invoke(null, ctx, lists, maxSizeList);
  }

  private static void setStaticField(String name, Object value) throws Exception {
    Field field = AbstractCli.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(null, value);
  }
}
