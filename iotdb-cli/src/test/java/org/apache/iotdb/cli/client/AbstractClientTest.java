/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.cli.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.iotdb.cli.client.AbstractClient.OPERATION_RESULT;
import org.apache.iotdb.cli.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBDatabaseMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class AbstractClientTest {

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
        String[] keywords = { AbstractClient.HOST_ARGS, AbstractClient.HELP_ARGS, AbstractClient.PORT_ARGS,
                AbstractClient.PASSWORD_ARGS, AbstractClient.USERNAME_ARGS, AbstractClient.ISO8601_ARGS,
                AbstractClient.MAX_PRINT_ROW_COUNT_ARGS, };
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
        String[] args = new String[] { "-u", "user1" };
        CommandLine commandLine = parser.parse(options, args);
        String str = AbstractClient.checkRequiredArg(AbstractClient.USERNAME_ARGS, AbstractClient.USERNAME_NAME,
                commandLine, true, "root");
        assertEquals(str, "user1");

        args = new String[] { "-u", "root", };
        commandLine = parser.parse(options, args);
        str = AbstractClient.checkRequiredArg(AbstractClient.HOST_ARGS, AbstractClient.HOST_NAME, commandLine, false,
                "127.0.0.1");
        assertEquals(str, "127.0.0.1");
        try {
            str = AbstractClient.checkRequiredArg(AbstractClient.HOST_ARGS, AbstractClient.HOST_NAME, commandLine, true,
                    "127.0.0.1");
        } catch (ArgsErrorException e) {
            assertEquals(e.getMessage(), "IoTDB: Required values for option 'host' not provided");
        }
        try {
            str = AbstractClient.checkRequiredArg(AbstractClient.HOST_ARGS, AbstractClient.HOST_NAME, commandLine,
                    false, null);
        } catch (ArgsErrorException e) {
            assertEquals(e.getMessage(), "IoTDB: Required values for option 'host' is null.");
        }
    }

    @Test
    public void testRemovePasswordArgs() {
        AbstractClient.init();
        String[] input = new String[] { "-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root" };
        String[] res = new String[] { "-h", "127.0.0.1", "-p", "6667", "-u", "root", "-pw", "root" };
        isTwoStringArrayEqual(res, AbstractClient.removePasswordArgs(input));

        input = new String[] { "-h", "127.0.0.1", "-p", "6667", "-pw", "root", "-u", "root" };
        res = new String[] { "-h", "127.0.0.1", "-p", "6667", "-pw", "root", "-u", "root" };
        isTwoStringArrayEqual(res, AbstractClient.removePasswordArgs(input));

        input = new String[] { "-h", "127.0.0.1", "-p", "6667", "root", "-u", "root", "-pw" };
        res = new String[] { "-h", "127.0.0.1", "-p", "6667", "root", "-u", "root" };
        isTwoStringArrayEqual(res, AbstractClient.removePasswordArgs(input));

        input = new String[] { "-h", "127.0.0.1", "-p", "6667", "-pw", "-u", "root" };
        res = new String[] { "-h", "127.0.0.1", "-p", "6667", "-u", "root" };
        isTwoStringArrayEqual(res, AbstractClient.removePasswordArgs(input));

        input = new String[] { "-pw", "-h", "127.0.0.1", "-p", "6667", "root", "-u", "root" };
        res = new String[] { "-h", "127.0.0.1", "-p", "6667", "root", "-u", "root" };
        isTwoStringArrayEqual(res, AbstractClient.removePasswordArgs(input));

        input = new String[] {};
        res = new String[] {};
        isTwoStringArrayEqual(res, AbstractClient.removePasswordArgs(input));
    }

    private void isTwoStringArrayEqual(String[] expected, String[] actual) {
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    @Test
    public void testHandleInputInputCmd() {
        assertEquals(AbstractClient.handleInputInputCmd(AbstractClient.EXIT_COMMAND, connection),
                OPERATION_RESULT.RETURN_OPER);
        assertEquals(AbstractClient.handleInputInputCmd(AbstractClient.QUIT_COMMAND, connection),
                OPERATION_RESULT.RETURN_OPER);

        assertEquals(AbstractClient.handleInputInputCmd(AbstractClient.SHOW_METADATA_COMMAND, connection),
                OPERATION_RESULT.CONTINUE_OPER);

        assertEquals(AbstractClient.handleInputInputCmd(String.format("%s=", AbstractClient.SET_TIMESTAMP_DISPLAY),
                connection), OPERATION_RESULT.CONTINUE_OPER);
        assertEquals(AbstractClient.handleInputInputCmd(String.format("%s=xxx", AbstractClient.SET_TIMESTAMP_DISPLAY),
                connection), OPERATION_RESULT.CONTINUE_OPER);
        assertEquals(AbstractClient
                .handleInputInputCmd(String.format("%s=default", AbstractClient.SET_TIMESTAMP_DISPLAY), connection),
                OPERATION_RESULT.CONTINUE_OPER);
        testSetTimeFormat();

        assertEquals(AbstractClient.handleInputInputCmd(String.format("%s=", AbstractClient.SET_MAX_DISPLAY_NUM),
                connection), OPERATION_RESULT.CONTINUE_OPER);
        assertEquals(AbstractClient.handleInputInputCmd(String.format("%s=xxx", AbstractClient.SET_MAX_DISPLAY_NUM),
                connection), OPERATION_RESULT.CONTINUE_OPER);
        assertEquals(AbstractClient.handleInputInputCmd(String.format("%s=1", AbstractClient.SET_MAX_DISPLAY_NUM),
                connection), OPERATION_RESULT.CONTINUE_OPER);
        testSetMaxDisplayNumber();

        assertEquals(AbstractClient.handleInputInputCmd(AbstractClient.SHOW_TIMEZONE, connection),
                OPERATION_RESULT.CONTINUE_OPER);
        assertEquals(AbstractClient.handleInputInputCmd(AbstractClient.SHOW_TIMESTAMP_DISPLAY, connection),
                OPERATION_RESULT.CONTINUE_OPER);
        assertEquals(AbstractClient.handleInputInputCmd(AbstractClient.SHOW_FETCH_SIZE, connection),
                OPERATION_RESULT.CONTINUE_OPER);

        assertEquals(AbstractClient.handleInputInputCmd(String.format("%s=", AbstractClient.SET_TIME_ZONE), connection),
                OPERATION_RESULT.CONTINUE_OPER);
        assertEquals(AbstractClient.handleInputInputCmd(String.format("%s=+08:00", AbstractClient.SET_TIME_ZONE),
                connection), OPERATION_RESULT.CONTINUE_OPER);

        assertEquals(
                AbstractClient.handleInputInputCmd(String.format("%s=", AbstractClient.SET_FETCH_SIZE), connection),
                OPERATION_RESULT.CONTINUE_OPER);
        assertEquals(
                AbstractClient.handleInputInputCmd(String.format("%s=111", AbstractClient.SET_FETCH_SIZE), connection),
                OPERATION_RESULT.CONTINUE_OPER);
    }

    private void testSetTimeFormat() {
        AbstractClient.setTimeFormat("long");
        assertEquals(AbstractClient.maxTimeLength, AbstractClient.maxValueLength);
        assertEquals(AbstractClient.formatTime, "%" + AbstractClient.maxTimeLength + "s|");

        AbstractClient.setTimeFormat("number");
        assertEquals(AbstractClient.maxTimeLength, AbstractClient.maxValueLength);
        assertEquals(AbstractClient.formatTime, "%" + AbstractClient.maxTimeLength + "s|");

        AbstractClient.setTimeFormat("default");
        assertEquals(AbstractClient.maxTimeLength, AbstractClient.ISO_DATETIME_LEN);
        assertEquals(AbstractClient.formatTime, "%" + AbstractClient.maxTimeLength + "s|");

        AbstractClient.setTimeFormat("iso8601");
        assertEquals(AbstractClient.maxTimeLength, AbstractClient.ISO_DATETIME_LEN);
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
        assertEquals(AbstractClient.maxPrintRowCount, 10);
        AbstractClient.setMaxDisplayNumber("111111111111111");
        assertEquals(AbstractClient.maxPrintRowCount, Integer.MAX_VALUE);
        AbstractClient.setMaxDisplayNumber("-10");
        assertEquals(AbstractClient.maxPrintRowCount, Integer.MAX_VALUE);
    }
}
