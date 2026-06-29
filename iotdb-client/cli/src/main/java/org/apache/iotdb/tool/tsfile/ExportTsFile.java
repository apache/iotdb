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

package org.apache.iotdb.tool.tsfile;

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.tool.common.Constants;
import org.apache.iotdb.tool.common.OptionsUtil;
import org.apache.iotdb.tool.tsfile.subscription.AbstractSubscriptionTsFile;
import org.apache.iotdb.tool.tsfile.subscription.CommonParam;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExportTsFile {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static CommonParam commonParam = CommonParam.getInstance();

  public static void main(String[] args) throws Exception {
    Logger logger =
        (Logger) LoggerFactory.getLogger("org.apache.iotdb.session.subscription.consumer.base");
    logger.setLevel(Level.WARN);
    Options options = OptionsUtil.createSubscriptionTsFileOptions();
    parseParams(args, options);
    if (StringUtils.isEmpty(commonParam.getPath())) {
      commonParam.setSqlDialect(Constants.TABLE_MODEL);
    }
    AbstractSubscriptionTsFile.setSubscriptionSession();
    String nowFormat = Constants.DATE_FORMAT_VIEW.format(System.currentTimeMillis());
    String topicName = Constants.TOPIC_NAME_PREFIX + nowFormat;
    String groupId = Constants.GROUP_NAME_PREFIX + nowFormat;
    commonParam.getSubscriptionTsFile().createTopics(topicName);
    commonParam.getSubscriptionTsFile().createConsumers(groupId);
    commonParam.getSubscriptionTsFile().subscribe(topicName);
    ExecutorService executor = Executors.newFixedThreadPool(commonParam.getConsumerCount());
    commonParam.getSubscriptionTsFile().consumerPoll(executor, topicName);
    executor.shutdown();
    while (true) {
      if (executor.isTerminated()) {
        break;
      }
    }
    commonParam.getSubscriptionTsFile().doClean();
    ioTPrinter.println("Export TsFile Count: " + commonParam.getCountFile().get());
  }

  private static void parseParams(String[] args, Options options) {
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    CommandLine cli = null;
    CommandLineParser cliParser = new DefaultParser();
    try {
      cli = cliParser.parse(options, args);
      if (cli.hasOption(Constants.HELP_ARGS) || args.length == 0) {
        hf.printHelp(Constants.SUBSCRIPTION_CLI_PREFIX, options, true);
        System.exit(0);
      }
      if (cli.hasOption(Constants.SQL_DIALECT_ARGS)) {
        commonParam.setSqlDialect(cli.getOptionValue(Constants.SQL_DIALECT_ARGS));
      }
      if (cli.hasOption(Constants.HOST_ARGS)) {
        commonParam.setSrcHost(cli.getOptionValue(Constants.HOST_ARGS));
      }
      if (cli.hasOption(Constants.PORT_ARGS)) {
        commonParam.setSrcPort(Integer.valueOf(cli.getOptionValue(Constants.PORT_ARGS)));
      }
      if (cli.hasOption(Constants.USERNAME_ARGS)) {
        commonParam.setSrcUserName(cli.getOptionValue(Constants.USERNAME_ARGS));
      }
      if (cli.hasOption(Constants.PW_ARGS)) {
        commonParam.setSrcPassword(cli.getOptionValue(Constants.PW_ARGS));
      }
      if (cli.hasOption(Constants.PATH_ARGS)) {
        commonParam.setPath(cli.getOptionValue(Constants.PATH_ARGS));
      }
      if (cli.hasOption(Constants.DB_ARGS)) {
        commonParam.setDatabase(cli.getOptionValue(Constants.DB_ARGS));
      }
      if (cli.hasOption(Constants.TABLE_ARGS)) {
        commonParam.setTable(cli.getOptionValue(Constants.TABLE_ARGS));
      }
      if (cli.hasOption(Constants.TARGET_DIR_ARGS)) {
        commonParam.setTargetDir(cli.getOptionValue(Constants.TARGET_DIR_ARGS));
      }
      if (cli.hasOption(Constants.START_TIME_ARGS)) {
        commonParam.setStartTime(cli.getOptionValue(Constants.START_TIME_ARGS));
      }
      if (cli.hasOption(Constants.END_TIME_ARGS)) {
        commonParam.setEndTime(cli.getOptionValue(Constants.END_TIME_ARGS));
      }
      if (cli.hasOption(Constants.THREAD_NUM_ARGS)) {
        commonParam.setConsumerCount(
            Integer.valueOf(cli.getOptionValue(Constants.THREAD_NUM_ARGS)));
      }
    } catch (ParseException e) {
      ioTPrinter.println(e.getMessage());
      hf.printHelp(Constants.SUBSCRIPTION_CLI_PREFIX, options, true);
      System.exit(Constants.CODE_ERROR);
    }
  }
}
