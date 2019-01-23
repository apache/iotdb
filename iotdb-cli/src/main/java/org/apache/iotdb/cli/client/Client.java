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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.iotdb.cli.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBConnection;

public class Client extends AbstractClient {

  private static final String[] FUNCTION_NAME_LIST = new String[]{"count", "max_time", "min_time",
      "max_value",
      "min_value", "subsequence_matching", // with parameter(s)
      "now()" // without parameter
  };

  private static final String[] KEY_WORD_LIST = new String[]{"SHOW", "SELECT", "DROP", "UPDATE",
      "DELETE", "CREATE",
      "INSERT", "INDEX", "TIMESERIES", "TIME", "TIMESTAMP", "VALUES", "FROM", "WHERE", "TO", "ON",
      "WITH",
      "USING", "AND", "OR", "USER", "ROLE", "EXIT", "QUIT", "IMPORT"};

  private static final String[] CONF_NAME_LIST = new String[]{ // set <key>=<value>
      "time_display_type", "time_zone", // specific format
      "fetch_size", "max_display_num", // integer
      "storage group to" // special
  };

  private static final String[] PARAM_NAME_LIST = new String[]{"datatype", "encoding", // list
      "window_length", // integer
  };

  private static final String[] DATA_TYPE_LIST = new String[]{"BOOLEAN", "INT32", "INT64", "INT96",
      "FLOAT",
      "DOUBLE", "TEXT", "FIXED_LEN_BYTE_ARRAY", "ENUMS", "BIGDECIMAL"};

  private static final String[] ENCODING_LIST = new String[]{"PLAIN", "PLAIN_DICTIONARY", "RLE",
      "DIFF", "TS_2DIFF",
      "BITMAP", "GORILLA"};

  /**
   * IoTDB CLI main function.
   *
   * @param args launch arguments
   * @throws ClassNotFoundException ClassNotFoundException
   */
  public static void main(String[] args) throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      System.out.println(
          "Require more params input, eg. ./start-client.sh(start-client.bat if Windows) "
              + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx.");
      System.out.println("For more information, please check the following hint.");
      hf.printHelp(SCRIPT_HINT, options, true);
      return;
    }
    init();
    args = removePasswordArgs(args);
    try {
      commandLine = parser.parse(options, args);
      if (commandLine.hasOption(HELP_ARGS)) {
        hf.printHelp(SCRIPT_HINT, options, true);
        return;
      }
      if (commandLine.hasOption(ISO8601_ARGS)) {
        setTimeFormat("long");
      }
      if (commandLine.hasOption(MAX_PRINT_ROW_COUNT_ARGS)) {
        try {
          setMaxDisplayNumber(commandLine.getOptionValue(MAX_PRINT_ROW_COUNT_ARGS));
        } catch (NumberFormatException e) {
          System.out.println(
              IOTDB_CLI_PREFIX + "> error format of max print row count, it should be number");
          return;
        }
      }
    } catch (ParseException e) {
      System.out.println(
          "Require more params input, eg. ./start-client.sh(start-client.bat if Windows) "
              + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx.");
      System.out.println("For more information, please check the following hint.");
      hf.printHelp(IOTDB_CLI_PREFIX, options, true);
      return;
    }

    try(ConsoleReader reader = new ConsoleReader()) {
      reader.setExpandEvents(false);
      String s;

      host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine, false, host);
      port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine, false, port);
      username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine, true, null);

      password = commandLine.getOptionValue(PASSWORD_ARGS);
      if (password == null) {
        password = reader.readLine("please input your password:", '\0');
      }
      try (IoTDBConnection connection = (IoTDBConnection) DriverManager.getConnection(Config.IOTDB_URL_PREFIX + host + ":" + port + "/", username, password)){
        properties = connection.getServerProperties();
        AGGREGRATE_TIME_LIST.addAll(properties.getSupportedTimeAggregationOperations());
        displayLogo(properties.getVersion());
        System.out.println(IOTDB_CLI_PREFIX + "> login successfully");
        while (true) {
          s = reader.readLine(IOTDB_CLI_PREFIX + "> ", null);
          if (s != null) {
            String[] cmds = s.trim().split(";");
            for (int i = 0; i < cmds.length; i++) {
              String cmd = cmds[i];
              if (cmd != null && !cmd.trim().equals("")) {
                OperationResult result = handleInputCmd(cmd, connection);
                switch (result) {
                  case RETURN_OPER:
                    return;
                  case CONTINUE_OPER:
                    continue;
                  default:
                    break;
                }
              }
            }
          }
        }
      } catch (SQLException e) {
        System.out.println(String.format("%s> %s Host is %s, port is %s.", IOTDB_CLI_PREFIX, e.getMessage(), host, port));
      }
    } catch(ArgsErrorException e){
      System.out.println(IOTDB_CLI_PREFIX + "> input params error because" + e.getMessage());
    }catch (Exception e) {
      System.out.println(IOTDB_CLI_PREFIX + "> exit client with error " + e.getMessage());
    }
  }

  @Deprecated
  private static Completer[] getCommandCompleter() {
    List<String> candidateStrings = new ArrayList<>();
    for (String s : FUNCTION_NAME_LIST) {
      if (!s.endsWith("()")) {
        candidateStrings.add(s + "(");
      } else { // for functions with no parameter, such as now().
        candidateStrings.add(s + " ");
      }
    }
    for (String s : KEY_WORD_LIST) {
      candidateStrings.add(s + " ");
      candidateStrings.add(s.toLowerCase() + " ");
    }
    StringsCompleter strCompleter = new StringsCompleter(candidateStrings);
    ArgumentCompleter.ArgumentDelimiter delim = new ArgumentCompleter.AbstractArgumentDelimiter() {
      @Override
      public boolean isDelimiterChar(CharSequence buffer, int pos) {
        char c = buffer.charAt(pos);
        return (Character.isWhitespace(c) || c == '(' || c == ')' || c == ',');
      }
    };
    final ArgumentCompleter argCompleter = new ArgumentCompleter(delim, strCompleter);
    argCompleter.setStrict(false);

    StringsCompleter confCompleter = new StringsCompleter(Arrays.asList(CONF_NAME_LIST)) {
      @Override
      public int complete(final String buffer, final int cursor,
          final List<CharSequence> candidates) {
        int result = super.complete(buffer, cursor, candidates);
        if (candidates.isEmpty() && cursor > 1 && buffer.charAt(cursor - 1) == '=') {
          String confName = buffer.substring(0, cursor - 1);
          switch (confName) { // TODO: give config suggestion
            default:
              break;
          }
          return cursor;
        }
        return result;
      }
    };
    StringsCompleter setCompleter = new StringsCompleter(Arrays.asList("set", "show")) {
      @Override
      public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        return buffer != null && (buffer.equals("set") || buffer.equals("show"))
            ? super.complete(buffer, cursor, candidates) : -1;
      }
    };
    ArgumentCompleter confPropCompleter = new ArgumentCompleter(setCompleter, confCompleter) {
      @Override
      public int complete(String buffer, int offset, List<CharSequence> completions) {
        int ret = super.complete(buffer, offset, completions);
        if (completions.size() == 1) {
          completions.set(0, ((String) completions.get(0)).trim());
        }
        return ret;
      }
    };

    StringsCompleter insertConfCompleter = new StringsCompleter(Arrays.asList("into")) {
      @Override
      public int complete(final String buffer, final int cursor,
          final List<CharSequence> candidates) {
        int result = super.complete(buffer, cursor, candidates);
        if (candidates.isEmpty() && cursor > 1 && buffer.charAt(cursor - 1) == '=') {
          String confName = buffer.substring(0, cursor - 1);
          switch (confName) { // TODO: give config suggestion
            default:
              break;
          }
          return cursor;
        }
        return result;
      }
    };
    StringsCompleter insertCompleter = new StringsCompleter(Arrays.asList("insert")) {
      @Override
      public int complete(String buffer, int cursor, List<CharSequence> candidates) {
        return buffer != null && (buffer.equals("insert")) ? super
            .complete(buffer, cursor, candidates) : -1;
      }
    };
    ArgumentCompleter insertPropCompleter = new ArgumentCompleter(insertCompleter,
        insertConfCompleter) {
      @Override
      public int complete(String buffer, int offset, List<CharSequence> completions) {
        int ret = super.complete(buffer, offset, completions);
        if (completions.size() == 1) {
          completions.set(0, ((String) completions.get(0)).trim());
        }
        return ret;
      }
    };

    StringsCompleter withParamCompleter = new StringsCompleter(Arrays.asList(PARAM_NAME_LIST)) {
      @Override
      public int complete(final String buffer, final int cursor,
          final List<CharSequence> candidates) {
        int result = super.complete(buffer, cursor, candidates);
        if (candidates.isEmpty() && cursor > 1) {
          int equalsIdx = buffer.indexOf('=');
          if (equalsIdx != -1) {
            String confName = buffer.substring(0, equalsIdx);
            String value = buffer.substring(equalsIdx + 1).toUpperCase();
            if (confName.startsWith("encoding")) {
              for (String str : ENCODING_LIST) {
                if (str.startsWith(value) && !str.equals(value)) {
                  candidates.add(str);
                }
              }
              return equalsIdx + 1;
            } else if (confName.startsWith("datatype")) {
              for (String str : DATA_TYPE_LIST) {
                if (str.startsWith(value) && !str.equals(value)) {
                  candidates.add(str);
                }
              }
              return equalsIdx + 1;
            }
            return cursor;
          }
        }
        return result;
      }
    };
    ArgumentCompleter withParamPropCompleter = new ArgumentCompleter(delim, withParamCompleter) {
      @Override
      public int complete(String buffer, int offset, List<CharSequence> completions) {
        int ret = super.complete(buffer, offset, completions);
        if (completions.size() == 1) {
          completions.set(0, ((String) completions.get(0)).trim());
        }
        return ret;
      }
    };
    withParamPropCompleter.setStrict(false);

    return new Completer[]{confPropCompleter, insertPropCompleter, withParamPropCompleter,
        argCompleter};
  }
}
