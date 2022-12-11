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
package org.apache.iotdb.db.tools.mlog;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MetadataConstant;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ExportSchema {
  private static final Logger logger = LoggerFactory.getLogger(ExportSchema.class);

  private static final String EXPORT_PREFIX = "ExportSchema";

  private static final String SOURCE_DIR_ARGS = "d";
  private static final String SOURCE_DIR_NAME = "source directory path";

  private static final String TARGET_DIR_ARGS = "o";
  private static final String TARGET_DIR_NAME = "target directory path";

  private static final String HELP_ARGS = "help";

  private final String sourceDir;
  private final String targetDir;

  private ExportSchema(String sourceDir, String targetDir) {
    this.sourceDir = sourceDir;
    this.targetDir = targetDir;
  }

  /**
   * create the commandline options.
   *
   * @return object Options
   */
  public static Options createOptions() {
    Options options = new Options();

    Option sourceDir =
        Option.builder(SOURCE_DIR_ARGS)
            .required()
            .argName(SOURCE_DIR_NAME)
            .hasArg()
            .desc("Need to specify a source directory path")
            .build();
    options.addOption(sourceDir);

    Option targetDir =
        Option.builder(TARGET_DIR_ARGS)
            .required()
            .argName(TARGET_DIR_NAME)
            .hasArg()
            .desc("Need to specify a target directory path")
            .build();
    options.addOption(targetDir);

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
    //    args =
    //        new String[] {
    //          "-d",
    //          "/Users/chenyanze/Desktop/exportSchema",
    //          "-o",
    //          "/Users/chenyanze/projects/JavaProjects/iotdb/iotdb/data/system/schema"
    //        };
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    CommandLine commandLine;
    CommandLineParser parser = new DefaultParser();
    if (args == null || args.length == 0) {
      logger.warn("Too few params input, please check the following hint.");
      hf.printHelp(EXPORT_PREFIX, options, true);
      return;
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error("Parse error: {}", e.getMessage());
      hf.printHelp(EXPORT_PREFIX, options, true);
      return;
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(EXPORT_PREFIX, options, true);
      return;
    }

    String sourceDir = commandLine.getOptionValue(SOURCE_DIR_ARGS);
    String targetDir = commandLine.getOptionValue(TARGET_DIR_ARGS);
    File srcDir = new File(sourceDir);
    if (!srcDir.exists() || !srcDir.isDirectory()) {
      logger.error(
          "Encounter an error, because: {} does not exist or is not a directory.", sourceDir);
    } else {
      File[] files =
          srcDir.listFiles(
              (dir, name) ->
                  MetadataConstant.METADATA_LOG.equals(name)
                      || MetadataConstant.TAG_LOG.equals(name));
      if (files == null || files.length != 2) {
        logger.error("Encounter an error, because: {} is not a valid directory.", sourceDir);
      } else {
        ExportSchema exportSchema = new ExportSchema(sourceDir, targetDir);
        try {
          exportSchema.export();
        } catch (Exception e) {
          logger.error("Encounter an error, because: {} ", e.getMessage());
        } finally {
          exportSchema.clear();
        }
      }
    }
  }

  private void export() throws Exception {

    IoTDBDescriptor.getInstance().getConfig().setSchemaDir(sourceDir);
    MManager.getInstance().init();
    MManager.getInstance().exportSchema(new File(targetDir));
  }

  public void clear() {
    MManager.getInstance().clear();
  }
}
