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
package org.apache.iotdb.cli.tool;

import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.iotdb.cli.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.thrift.TException;

import java.io.IOException;
import java.time.ZoneId;

public abstract class AbstractCsvTool {
    protected static final String HOST_ARGS = "h";
    protected static final String HOST_NAME = "host";

    protected static final String HELP_ARGS = "help";

    protected static final String PORT_ARGS = "p";
    protected static final String PORT_NAME = "port";

    protected static final String PASSWORD_ARGS = "pw";
    protected static final String PASSWORD_NAME = "password";

    protected static final String USERNAME_ARGS = "u";
    protected static final String USERNAME_NAME = "username";

    protected static final String TIME_FORMAT_ARGS = "tf";
    protected static final String TIME_FORMAT_NAME = "timeformat";

    protected static final String TIME_ZONE_ARGS = "tz";
    protected static final String TIME_ZONE_NAME = "timeZone";

    protected static String host;
    protected static String port;
    protected static String username;
    protected static String password;

    protected static final int MAX_HELP_CONSOLE_WIDTH = 92;

    protected static ZoneId zoneId;
    protected static String timeZoneID;
    protected static String timeFormat;

    // protected static final String JDBC_DRIVER = "case hu.tsfiledb.jdbc.TsfileDriver";

    protected static final String DEFAULT_TIME_FORMAT = "ISO8601";

    protected static final String[] SUPPORT_TIME_FORMAT = new String[] { DEFAULT_TIME_FORMAT, "default", "long",
            "number", "timestamp", "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "yyyy.MM.dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss", "yyyy/MM/dd'T'HH:mm:ss", "yyyy.MM.dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ssZZ",
            "yyyy/MM/dd HH:mm:ssZZ", "yyyy.MM.dd HH:mm:ssZZ", "yyyy-MM-dd'T'HH:mm:ssZZ", "yyyy/MM/dd'T'HH:mm:ssZZ",
            "yyyy.MM.dd'T'HH:mm:ssZZ", "yyyy/MM/dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy.MM.dd HH:mm:ss.SSS",
            "yyyy/MM/dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy.MM.dd'T'HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss.SSSZZ", "yyyy/MM/dd HH:mm:ss.SSSZZ",
            "yyyy.MM.dd HH:mm:ss.SSSZZ", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ", "yyyy/MM/dd'T'HH:mm:ss.SSSZZ", };

    protected static IoTDBConnection connection;

    protected static String checkRequiredArg(String arg, String name, CommandLine commandLine)
            throws ArgsErrorException {
        String str = commandLine.getOptionValue(arg);
        if (str == null) {
            String msg = String.format("Required values for option '%s' not provided", name);
            System.out.println(msg);
            System.out.println("Use -help for more information");
            throw new ArgsErrorException(msg);
        }
        return str;
    }

    protected static void setTimeZone() throws IoTDBSQLException, TException {
        if (timeZoneID != null) {
            connection.setTimeZone(timeZoneID);
        }
        zoneId = ZoneId.of(connection.getTimeZone());
    }

    protected static void parseBasicParams(CommandLine commandLine, ConsoleReader reader)
            throws ArgsErrorException, IOException {
        host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine);
        port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine);
        username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine);

        password = commandLine.getOptionValue(PASSWORD_ARGS);
        if (password == null) {
            password = reader.readLine("please input your password:", '\0');
        }
    }

    protected static boolean checkTimeFormat() {
        for (String format : SUPPORT_TIME_FORMAT) {
            if (timeFormat.equals(format)) {
                return true;
            }
        }
        System.out.println(String.format("Input time format %s is not supported, "
                + "please input like yyyy-MM-dd\\ HH:mm:ss.SSS or yyyy-MM-dd'T'HH:mm:ss.SSS", timeFormat));
        return false;
    }
}
