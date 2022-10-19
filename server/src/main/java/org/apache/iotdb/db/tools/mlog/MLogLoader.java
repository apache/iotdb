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

import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.tag.TagManager;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ActivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AppendTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeTagOffsetPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeactivateTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.DropTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.PruneTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.utils.CommandLineUtils;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.util.Version;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Get a mlog.bin file in an IoTDB, and load this mlog.bin into another IoTDB. This tool receives a
 * mlog.bin and IoTDB ip, port. It reads this mlog.bin and use session interface to replay.
 */
public class MLogLoader {
  private static final Logger logger = LoggerFactory.getLogger(MLogLoader.class);
  private static final String MLOG_LOAD_PREFIX = "MLogLoad";

  private static final String MLOG_FILE_ARGS = "mlog";
  private static final String MLOG_FILE_NAME = "mlog file";

  private static final String TLOG_FILE_ARGS = "tlog";
  private static final String TLOG_FILE_NAME = "tlog file";

  private static final String PORT_ARGS = "p";
  private static final String PORT_NAME = "receiver port";

  private static final String HOST_ARGS = "h";
  private static final String HOST_NAME = "receiver host";

  private static final String USER_ARGS = "u";
  private static final String USER_NAME = "user";

  private static final String PASSWORD_ARGS = "pw";
  private static final String PASSWORD_NAME = "password";

  private static final String HELP_ARGS = "help";

  private static String mLogFile;
  private static String tLogFile;
  private static String host;
  private static int port;
  private static String user;
  private static String password;
  private static TagManager tagManager;

  /**
   * create the commandline options.
   *
   * @return object Options
   */
  public static Options createOptions() {
    Options options = new Options();

    Option opMlog =
        Option.builder(MLOG_FILE_ARGS)
            .required()
            .argName(MLOG_FILE_NAME)
            .hasArg()
            .desc("Need to specify a binary mlog.bin file to parse (required)")
            .build();
    options.addOption(opMlog);

    Option opTlog =
        Option.builder(TLOG_FILE_ARGS)
            .required(false)
            .argName(TLOG_FILE_NAME)
            .hasArg()
            .desc(
                "Could specify a binary tlog.txt file to parse. Tags and attributes will be ignored if not specified (optional)")
            .build();
    options.addOption(opTlog);

    Option opHost =
        Option.builder(HOST_ARGS)
            .required(false)
            .argName(HOST_NAME)
            .hasArg()
            .desc("Could specify a specify the receiver host, default is 127.0.0.1 (optional)")
            .build();
    options.addOption(opHost);

    Option opPort =
        Option.builder(PORT_ARGS)
            .required(false)
            .argName(PORT_NAME)
            .hasArg()
            .desc("Could specify a specify the receiver port, default is 6667 (optional)")
            .build();
    options.addOption(opPort);

    Option opUser =
        Option.builder(USER_ARGS)
            .required(false)
            .argName(USER_NAME)
            .hasArg()
            .desc("Could specify the user name, default is root (optional)")
            .build();
    options.addOption(opUser);

    Option opPw =
        Option.builder(PASSWORD_ARGS)
            .required(false)
            .argName(PASSWORD_NAME)
            .hasArg()
            .desc("Could specify the password, default is root (optional)")
            .build();
    options.addOption(opPw);

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
      hf.printHelp(MLOG_LOAD_PREFIX, options, true);
      return;
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error("Parse error: {}", e.getMessage());
      hf.printHelp(MLOG_LOAD_PREFIX, options, true);
      return;
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(MLOG_LOAD_PREFIX, options, true);
      return;
    }
    try {
      parseBasicParams(commandLine);
      parseFileAndLoad();
    } catch (Exception e) {
      logger.error("Encounter an error, because: {} ", e.getMessage());
    }
  }

  public static void parseBasicParams(CommandLine commandLine) throws ParseException, IOException {
    mLogFile = CommandLineUtils.checkRequiredArg(MLOG_FILE_ARGS, MLOG_FILE_NAME, commandLine);
    host = commandLine.getOptionValue(HOST_ARGS);
    if (host == null) {
      host = "127.0.0.1";
    }
    tLogFile = commandLine.getOptionValue(TLOG_FILE_ARGS);
    String portTmp = commandLine.getOptionValue(PORT_ARGS);
    port = portTmp == null ? 6667 : Integer.parseInt(portTmp);
    user = commandLine.getOptionValue(USER_ARGS);
    if (user == null) {
      user = "root";
    }
    password = commandLine.getOptionValue(PASSWORD_ARGS);
    if (password == null) {
      password = "root";
    }
  }

  public static void parseFileAndLoad() throws Exception {
    Session session =
        new Session.Builder()
            .host(host)
            .port(port)
            .username(user)
            .password(password)
            .version(Version.V_0_13)
            .build();
    try (MLogReader mLogReader = new MLogReader(mLogFile)) {
      session.open(false);
      while (mLogReader.hasNext()) {
        PhysicalPlan plan = mLogReader.next();
        logger.info("Start load plan {}", plan);
        try {
          switch (plan.getOperatorType()) {
            case CREATE_TIMESERIES:
              CreateTimeSeriesPlan createTimeSeriesPlan = (CreateTimeSeriesPlan) plan;
              if (createTimeSeriesPlan.getTagOffset() != -1) {
                if (tLogFile == null) {
                  logger.warn(
                      "No specify tlog.txt file to parse, tag and attributes will be ignored.");
                  createTimeSeriesPlan.setTags(Collections.emptyMap());
                  createTimeSeriesPlan.setAttributes(Collections.emptyMap());
                } else {
                  fillTagsAndOffset(createTimeSeriesPlan);
                }
              }
              session.createTimeseries(
                  createTimeSeriesPlan.getPath().getFullPath(),
                  createTimeSeriesPlan.getDataType(),
                  createTimeSeriesPlan.getEncoding(),
                  createTimeSeriesPlan.getCompressor(),
                  createTimeSeriesPlan.getProps(),
                  createTimeSeriesPlan.getTags(),
                  createTimeSeriesPlan.getAttributes(),
                  createTimeSeriesPlan.getAlias());
              break;
            case CREATE_ALIGNED_TIMESERIES:
              CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan =
                  (CreateAlignedTimeSeriesPlan) plan;
              session.createAlignedTimeseries(
                  createAlignedTimeSeriesPlan.getPrefixPath().getFullPath(),
                  createAlignedTimeSeriesPlan.getMeasurements(),
                  createAlignedTimeSeriesPlan.getDataTypes(),
                  createAlignedTimeSeriesPlan.getEncodings(),
                  createAlignedTimeSeriesPlan.getCompressors(),
                  createAlignedTimeSeriesPlan.getAliasList());
              break;
            case DELETE_TIMESERIES:
              session.deleteTimeseries(
                  plan.getPaths().stream()
                      .map(PartialPath::getFullPath)
                      .collect(Collectors.toList()));
              break;
            case SET_STORAGE_GROUP:
              session.setStorageGroup(((SetStorageGroupPlan) plan).getPath().getFullPath());
              break;
            case DELETE_STORAGE_GROUP:
              session.deleteStorageGroups(
                  plan.getPaths().stream()
                      .map(PartialPath::getFullPath)
                      .collect(Collectors.toList()));
              break;
            case TTL:
              SetTTLPlan setTTLPlan = (SetTTLPlan) plan;
              if (setTTLPlan.getDataTTL() == Long.MAX_VALUE) {
                session.executeNonQueryStatement(
                    String.format("unset ttl to %s", setTTLPlan.getStorageGroup()));
              } else {
                session.executeNonQueryStatement(
                    String.format(
                        "set ttl to %s %d", setTTLPlan.getStorageGroup(), setTTLPlan.getDataTTL()));
              }
              break;
            case CHANGE_ALIAS:
              ChangeAliasPlan changeAliasPlan = (ChangeAliasPlan) plan;
              session.executeNonQueryStatement(
                  String.format(
                      "ALTER timeseries %s UPSERT ALIAS=%s",
                      changeAliasPlan.getPath(), changeAliasPlan.getAlias()));
              break;
            case CHANGE_TAG_OFFSET:
              if (tLogFile == null) {
                logger.warn("No specify tlog.txt file to parse, skip ChangeTagOffsetPlan.");
              } else {
                session.executeNonQueryStatement(genAlterTimeSeriesSQL((ChangeTagOffsetPlan) plan));
              }
              break;
            case CREATE_TEMPLATE:
              Template template = new Template((CreateTemplatePlan) plan);
              // currently, template must be flat
              List<String> measurements = new ArrayList<>();
              List<TSDataType> dataTypes = new ArrayList<>();
              List<TSEncoding> encodings = new ArrayList<>();
              List<CompressionType> compressors = new ArrayList<>();
              for (IMeasurementSchema measurementSchema : template.getSchemaMap().values()) {
                measurements.add(measurementSchema.getMeasurementId());
                dataTypes.add(measurementSchema.getType());
                encodings.add(measurementSchema.getEncodingType());
                compressors.add(measurementSchema.getCompressor());
              }
              session.createSchemaTemplate(
                  template.getName(),
                  measurements,
                  dataTypes,
                  encodings,
                  compressors,
                  template.isDirectAligned());
              break;
            case APPEND_TEMPLATE:
              AppendTemplatePlan appendTemplatePlan = (AppendTemplatePlan) plan;
              if (appendTemplatePlan.isAligned()) {
                session.addAlignedMeasurementsInTemplate(
                    appendTemplatePlan.getName(),
                    appendTemplatePlan.getMeasurements(),
                    appendTemplatePlan.getDataTypes(),
                    appendTemplatePlan.getEncodings(),
                    appendTemplatePlan.getCompressors());
              } else {
                session.addUnalignedMeasurementsInTemplate(
                    appendTemplatePlan.getName(),
                    appendTemplatePlan.getMeasurements(),
                    appendTemplatePlan.getDataTypes(),
                    appendTemplatePlan.getEncodings(),
                    appendTemplatePlan.getCompressors());
              }
              break;
            case PRUNE_TEMPLATE:
              PruneTemplatePlan pruneTemplatePlan = (PruneTemplatePlan) plan;
              session.deleteNodeInTemplate(
                  pruneTemplatePlan.getName(), pruneTemplatePlan.getPrunedMeasurements().get(0));
              break;
            case SET_TEMPLATE:
              SetTemplatePlan setTemplatePlan = (SetTemplatePlan) plan;
              session.setSchemaTemplate(
                  setTemplatePlan.getTemplateName(), setTemplatePlan.getPrefixPath());
              break;
            case UNSET_TEMPLATE:
              UnsetTemplatePlan unsetTemplatePlan = (UnsetTemplatePlan) plan;
              session.unsetSchemaTemplate(
                  unsetTemplatePlan.getPrefixPath(), unsetTemplatePlan.getTemplateName());
              break;
            case DROP_TEMPLATE:
              session.dropSchemaTemplate(((DropTemplatePlan) plan).getName());
              break;
            case ACTIVATE_TEMPLATE:
              session.createTimeseriesOfTemplateOnPath(
                  ((ActivateTemplatePlan) plan).getPrefixPath().getFullPath());
              break;
            case DEACTIVATE_TEMPLATE:
              DeactivateTemplatePlan deactivateTemplatePlan = (DeactivateTemplatePlan) plan;
              session.deactivateTemplateOn(
                  deactivateTemplatePlan.getTemplateName(),
                  deactivateTemplatePlan.getPrefixPath().getFullPath());
              break;
            default:
              logger.warn("Skip load plan {}", plan);
          }
        } catch (Exception e) {
          logger.error("Fail to load plan {} because {}", plan, e.getMessage());
        }
      }
    } finally {
      if (tagManager != null) {
        tagManager.clear();
      }
      session.close();
    }
  }

  private static void fillTagsAndOffset(CreateTimeSeriesPlan createTimeSeriesPlan)
      throws IOException {
    if (tagManager == null) {
      tagManager = TagManager.getNewInstanceForMLogLoader(new File(tLogFile));
    }
    Pair<Map<String, String>, Map<String, String>> tagAndAttributePair =
        tagManager.readTagFile(createTimeSeriesPlan.getTagOffset());
    createTimeSeriesPlan.setTags(tagAndAttributePair.left);
    createTimeSeriesPlan.setAttributes(tagAndAttributePair.right);
  }

  private static String genAlterTimeSeriesSQL(ChangeTagOffsetPlan changeTagOffsetPlan)
      throws IOException {
    if (tagManager == null) {
      tagManager = TagManager.getNewInstanceForMLogLoader(new File(tLogFile));
    }
    Pair<Map<String, String>, Map<String, String>> tagAndAttributePair =
        tagManager.readTagFile(changeTagOffsetPlan.getOffset());
    StringBuilder stringBuilder =
        new StringBuilder(
            String.format("ALTER timeseries %s UPSERT", changeTagOffsetPlan.getPath()));
    if (tagAndAttributePair.left.size() > 0) {
      stringBuilder.append(" TAGS(");
      stringBuilder.append(
          StringUtils.join(
              tagAndAttributePair.left.entrySet().stream()
                  .map(i -> i.getKey() + "=" + i.getValue())
                  .toArray(),
              ", "));
      stringBuilder.append(")");
    }
    if (tagAndAttributePair.right.size() > 0) {
      stringBuilder.append(" ATTRIBUTES(");
      stringBuilder.append(
          StringUtils.join(
              tagAndAttributePair.right.entrySet().stream()
                  .map(i -> i.getKey() + "=" + i.getValue())
                  .toArray(),
              ", "));
      stringBuilder.append(")");
    }
    return stringBuilder.toString();
  }
}
