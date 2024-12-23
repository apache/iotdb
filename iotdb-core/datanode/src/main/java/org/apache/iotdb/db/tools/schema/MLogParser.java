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

package org.apache.iotdb.db.tools.schema;

import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.BufferedSerializer;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.FakeCRC32Deserializer;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.SchemaLogReader;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.SchemaLogWriter;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.visitor.SchemaRegionPlanDeserializer;
import org.apache.iotdb.db.schemaengine.schemaregion.logfile.visitor.SchemaRegionPlanTxtSerializer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** parse the binary mlog or snapshot to text */
public class MLogParser {

  private static final Logger logger = LoggerFactory.getLogger(MLogParser.class);
  private static final String MLOG_CLI_PREFIX = "print-schema-log";

  private static final String FILE_ARGS = "f";
  private static final String FILE_NAME = "mlog file";

  private static final String OUT_ARGS = "o";
  private static final String OUT_NAME = "output txt file";

  private static final String HELP_ARGS = "help";

  private static String inputFile;
  private static String outputFile;

  /**
   * create the commandline options.
   *
   * @return object Options
   */
  public static Options createOptions() {
    Options options = new Options();

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .argName(FILE_NAME)
            .hasArg()
            .desc("Need to specify a binary mlog file to parse (required)")
            .build();
    options.addOption(opFile);

    Option opOut =
        Option.builder(OUT_ARGS)
            .required(false)
            .argName(OUT_NAME)
            .hasArg()
            .desc("Could specify the output file after parse (optional)")
            .build();
    options.addOption(opOut);

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
            .desc("Display help information")
            .build();
    options.addOption(opHelp);

    return options;
  }

  public static void main(String[] args) {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    CommandLine commandLine;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      logger.warn("Too few params input, please check the following hint.");
      hf.printHelp(MLOG_CLI_PREFIX, options, true);
      return;
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error("Parse error: {}", e.getMessage());
      hf.printHelp(MLOG_CLI_PREFIX, options, true);
      return;
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(MLOG_CLI_PREFIX, options, true);
      return;
    }

    try {
      parseBasicParams(commandLine);
      parseFromFile(inputFile, outputFile);
    } catch (Exception e) {
      logger.error("Encounter an error, because: {} ", e.getMessage());
    }
  }

  public static void parseBasicParams(CommandLine commandLine) throws ParseException {
    inputFile = checkRequiredArg(FILE_ARGS, FILE_NAME, commandLine);
    outputFile = commandLine.getOptionValue(OUT_ARGS);

    if (outputFile == null) {
      outputFile = "tmp.txt";
    }
  }

  public static String checkRequiredArg(String arg, String name, CommandLine commandLine)
      throws ParseException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      String msg = String.format("Required values for option '%s' not provided", name);
      logger.info(msg);
      logger.info("Use -help for more information");
      throw new ParseException(msg);
    }
    return str;
  }

  public static void parseFromFile(String inputFile, String outputFile) throws IOException {
    try (SchemaLogReader<ISchemaRegionPlan> mLogReader =
            new SchemaLogReader<>(
                inputFile, new FakeCRC32Deserializer<>(new SchemaRegionPlanDeserializer()));
        SchemaLogWriter<ISchemaRegionPlan> mLogTxtWriter =
            new SchemaLogWriter<>(
                outputFile, new BufferedSerializer<>(new SchemaRegionPlanTxtSerializer()), false)) {
      ISchemaRegionPlan plan;
      while (mLogReader.hasNext()) {
        plan = mLogReader.next();
        mLogTxtWriter.write(plan);
      }
      mLogTxtWriter.force();
    }
  }
}
