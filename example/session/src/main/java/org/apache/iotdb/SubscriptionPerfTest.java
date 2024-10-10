package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.AckStrategy;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.apache.iotdb.session.subscription.model.Topic;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionSessionDataSet;
import org.apache.iotdb.session.subscription.payload.SubscriptionTsFileHandler;

import org.apache.commons.cli.*;
import org.apache.tsfile.write.record.Tablet;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.System.exit;
import static java.lang.System.out;

public class SubscriptionPerfTest {
  private static int topicCount = 1;
  private static int consumerCount = 1;
  private static int groupCount = 1;
  private static int databases = 1;
  private static int devices = 0;
  private static int timeseries = 0;
  private static final String topicNamePrefix = "topic_PerfTest_";
  private static final String groupNamePrefix = "group_PerfTest_";
  private static final String consumerNamePrefix = "consumer_PerfTest_";
  private static String consumerType = "pull";
  private static int PORT = 6667;
  private static SubscriptionSession subs;
  private static final SimpleDateFormat dateFormat =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private static String patternType = "root";
  private static String format = "SessionDataSetsHandler";
  private static String srcHost = "";
  private static String targetHost = "";
  private static String looseRange = "";
  private static String startTime = "";
  private static String endTime = "";
  private static String mode = "live";
  private static String targetDir = "target/out";

  private static SessionPool pool;

  private static boolean noCreate = false;
  private static boolean doClean = false;

  private static List<SubscriptionPullConsumer> pullConsumers;
  private static List<SubscriptionPushConsumer> pushConsumers;

  private static void createTopics() throws IoTDBConnectionException, StatementExecutionException {
    if (!noCreate) {
      Properties properties;
      System.out.println(dateFormat.format(new Date()) + " #### create topic start ####");
      int index = 0;
      switch (patternType) {
        case "ts":
          for (int i = 0; i < databases; i++) {
            for (int j = 0; j < devices; j++) {
              for (int k = 0; k < timeseries; k++) {
                if (index >= topicCount) break;
                properties = new Properties();
                properties.put(TopicConstant.MODE_KEY, mode);
                properties.put(TopicConstant.FORMAT_KEY, format);
                properties.put(TopicConstant.LOOSE_RANGE_KEY, looseRange);
                if (!startTime.isEmpty()) properties.put(TopicConstant.START_TIME_KEY, startTime);
                if (!startTime.isEmpty()) properties.put(TopicConstant.END_TIME_KEY, endTime);
                properties.put(TopicConstant.PATH_KEY, "root.test.g_" + i + ".d_" + j + ".s_" + k);
                subs.createTopic(topicNamePrefix + index, properties);
                index++;
              }
            }
          }
          break;
        case "ts1":
          int k = 0;
          for (int i = 0; i < databases; i++) {
            for (int j = 0; j < devices; j++) {
              if (index >= topicCount) break;
              properties = new Properties();
              properties.put(TopicConstant.MODE_KEY, mode);
              properties.put(TopicConstant.FORMAT_KEY, format);
              properties.put(TopicConstant.LOOSE_RANGE_KEY, looseRange);
              if (!startTime.isEmpty()) properties.put(TopicConstant.START_TIME_KEY, startTime);
              if (!startTime.isEmpty()) properties.put(TopicConstant.END_TIME_KEY, endTime);
              properties.put(TopicConstant.PATH_KEY, "root.test.g_" + i + ".d_" + j + ".s_" + k);
              subs.createTopic(topicNamePrefix + index, properties);
              index++;
            }
          }
          break;
        case "device":
          for (int i = 0; i < databases; i++) {
            for (int j = 0; j < devices; j++) {
              if (index >= topicCount) break;
              properties = new Properties();
              properties.put(TopicConstant.MODE_KEY, mode);
              properties.put(TopicConstant.FORMAT_KEY, format);
              properties.put(TopicConstant.LOOSE_RANGE_KEY, looseRange);
              if (!startTime.isEmpty()) properties.put(TopicConstant.START_TIME_KEY, startTime);
              if (!startTime.isEmpty()) properties.put(TopicConstant.END_TIME_KEY, endTime);
              properties.put(TopicConstant.PATH_KEY, "root.test.g_" + i + ".d_" + j + ".**");
              subs.createTopic(topicNamePrefix + index, properties);
              index++;
            }
          }
          break;
        case "db":
          for (int i = 0; i < databases; i++) {
            if (index >= topicCount) break;
            properties = new Properties();
            properties.put(TopicConstant.MODE_KEY, mode);
            properties.put(TopicConstant.FORMAT_KEY, format);
            properties.put(TopicConstant.LOOSE_RANGE_KEY, looseRange);
            if (!startTime.isEmpty()) properties.put(TopicConstant.START_TIME_KEY, startTime);
            if (!startTime.isEmpty()) properties.put(TopicConstant.END_TIME_KEY, endTime);
            properties.put(TopicConstant.PATH_KEY, "root.test.g_" + i + ".**");
            subs.createTopic(topicNamePrefix + index, properties);
            index++;
          }
          break;
        case "root":
          properties = new Properties();
          properties.put(TopicConstant.MODE_KEY, mode);
          properties.put(TopicConstant.FORMAT_KEY, format);
          properties.put(TopicConstant.LOOSE_RANGE_KEY, looseRange);
          if (!startTime.isEmpty()) properties.put(TopicConstant.START_TIME_KEY, startTime);
          if (!startTime.isEmpty()) properties.put(TopicConstant.END_TIME_KEY, endTime);
          properties.put(TopicConstant.PATH_KEY, "root.**");
          subs.createTopic(topicNamePrefix + "0", properties);
          break;
        default:
          properties = new Properties();
          properties.put(TopicConstant.MODE_KEY, mode);
          properties.put(TopicConstant.FORMAT_KEY, format);
          properties.put(TopicConstant.LOOSE_RANGE_KEY, looseRange);
          if (!startTime.isEmpty()) properties.put(TopicConstant.START_TIME_KEY, startTime);
          if (!startTime.isEmpty()) properties.put(TopicConstant.END_TIME_KEY, endTime);
          properties.put(TopicConstant.PATTERN_KEY, patternType);
          subs.createTopic(topicNamePrefix + "0", properties);
          break;
      }

      subs.getTopics().forEach(System.out::println);
      System.out.println(dateFormat.format(new Date()) + " #### create topic end ####");
    }
  }

  private static void createConsumers()
      throws IoTDBConnectionException, StatementExecutionException {
    System.out.println(dateFormat.format(new Date()) + " #### create consumer start ####");
    if ("pull".equals(consumerType)) {
      pullConsumers = new ArrayList<>(consumerCount);
      for (long i = 0; i < consumerCount; i++) {
        pullConsumers.add(
            new SubscriptionPullConsumer.Builder()
                .host(srcHost)
                .port(PORT)
                .consumerId(consumerNamePrefix + i)
                .consumerGroupId(groupNamePrefix + i % groupCount)
                .autoCommit(true)
                .autoCommitIntervalMs(20000)
                .fileSaveDir(targetDir)
                .buildPullConsumer());
      }
      if (doClean) {
        out.println(dateFormat.format(new Date()) + "#### close pull consumers ####");
        for (SubscriptionPullConsumer consumer : pullConsumers) {
          consumer.open();
          consumer.close();
        }
        out.println("#### Done ####");
      } else {
        out.println(dateFormat.format(new Date()) + "#### create pull consumers ####");
        for (SubscriptionPullConsumer consumer : pullConsumers) {
          consumer.open();
        }
      }
    } else {
      pushConsumers = new ArrayList<>(consumerCount);
      if ("SessionDataSetsHandler".equals(format)) {
        for (long i = 0; i < consumerCount; i++) {
          pushConsumers.add(
              new SubscriptionPushConsumer.Builder()
                  .host(srcHost)
                  .port(PORT)
                  .consumerId(consumerNamePrefix + i)
                  .consumerGroupId(groupNamePrefix + i % groupCount)
                  .ackStrategy(AckStrategy.BEFORE_CONSUME)
                  .consumeListener(
                      message -> {
                        for (final SubscriptionSessionDataSet dataSet :
                            message.getSessionDataSetsHandler()) {
                          try {
                            //
                            // System.out.println(dateFormat.format(new Date()) + "," +
                            // dataSet.getTablet().rowSize + "," +
                            // dataSet.getTablet().timestamps[0]);
                            pool.insertTablet(dataSet.getTablet());
                          } catch (StatementExecutionException e) {
                            throw new RuntimeException(e);
                          } catch (IoTDBConnectionException e) {
                            throw new RuntimeException(e);
                          }
                        }
                        return ConsumeResult.SUCCESS;
                      })
                  .buildPushConsumer());
        }
      } else {
        for (long i = 0; i < consumerCount; i++) {
          pushConsumers.add(
              new SubscriptionPushConsumer.Builder()
                  .host(srcHost)
                  .port(PORT)
                  .consumerId(consumerNamePrefix + i)
                  .consumerGroupId(groupNamePrefix + i % groupCount)
                  .ackStrategy(AckStrategy.BEFORE_CONSUME)
                  .consumeListener(
                      message -> {
                        return ConsumeResult.SUCCESS;
                      })
                  .buildPushConsumer());
        }
      }
      if (doClean) {
        out.println(dateFormat.format(new Date()) + "#### close push consumers ####");
        for (SubscriptionPushConsumer consumer : pushConsumers) {
          consumer.open();
          consumer.close();
        }
        out.println("#### Done ####");
      } else {
        out.println(dateFormat.format(new Date()) + "#### create push consumers ####");
        for (SubscriptionPushConsumer consumer : pushConsumers) {
          consumer.open();
        }
      }
    }
    subs.getSubscriptions().forEach(System.out::println);
    System.out.println(dateFormat.format(new Date()) + " #### create consumer end ####");
  }

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    Options options = new Options();
    Option opt1 = new Option("h", "sender_host", true, "the iotdb host of pipe sender");
    opt1.setRequired(true);
    options.addOption(opt1);
    Option opt2 =
        new Option(
            "t",
            "pattern_type",
            true,
            "the pattern type of pipe, Options: root,db,device,ts,ts1,pathValue, default "
                + patternType);
    opt2.setRequired(true);
    options.addOption(opt2);
    Option opt31 = new Option("u", "receiver_host", true, "the sink targetUrls of pipe");
    opt31.setRequired(true);
    options.addOption(opt31);
    Option opt4 = new Option("c", "topic_count", true, "the topic count, default " + topicCount);
    opt4.setRequired(false);
    options.addOption(opt4);
    Option opt5 = new Option("g", "databases", true, "the database count");
    opt5.setRequired(false);
    options.addOption(opt5);
    Option opt6 = new Option("d", "devices", true, "the device count");
    opt6.setRequired(false);
    options.addOption(opt6);
    Option opt7 = new Option("s", "timeseries", true, "the timeseries count per device ");
    opt7.setRequired(false);
    options.addOption(opt7);
    Option opt8 =
        new Option(
            "f", "format", true, "format: SessionDataSetsHandler,TsFileHandler, default " + format);
    opt8.setRequired(false);
    options.addOption(opt8);
    Option opt9 = new Option("l", "loose_range", true, "loose_range: path,time,all, default ''");
    opt9.setRequired(false);
    options.addOption(opt9);
    Option opt10 = new Option("b", "start_time", true, "filter: start time");
    opt10.setRequired(false);
    options.addOption(opt10);
    Option opt11 = new Option("e", "end_time", true, "filter: end time");
    opt11.setRequired(false);
    options.addOption(opt11);
    Option opt12 = new Option("m", "mode", true, "mode: live,snapshot default " + mode);
    opt12.setRequired(false);
    options.addOption(opt12);
    Option opt13 = new Option("a", "target_dir", true, "target_dir: put tsfile");
    opt13.setRequired(false);
    options.addOption(opt13);
    Option opt14 = new Option(null, "clean", false, "clean consumers and topics");
    opt14.setRequired(false);
    options.addOption(opt14);
    Option opt15 = new Option(null, "group_count", true, "the group count, default " + groupCount);
    opt15.setRequired(false);
    options.addOption(opt15);
    Option opt16 =
        new Option(null, "consumer_count", true, "the consumer count, default " + consumerCount);
    opt16.setRequired(false);
    options.addOption(opt16);
    Option opt17 =
        new Option(
            null,
            "consumer_type",
            true,
            "consumer type, optional:push,pull, default " + consumerType);
    opt17.setRequired(false);
    options.addOption(opt17);
    Option opt18 = new Option("n", "no_create", false, "do not create any topic");
    opt18.setRequired(false);
    options.addOption(opt18);
    Option opt22 = new Option("", "help", false, " show how to use");
    opt22.setRequired(false);
    options.addOption(opt22);

    CommandLine cli = null;
    CommandLineParser cliParser = new DefaultParser();
    HelpFormatter helpFormatter = new HelpFormatter();

    try {
      cli = cliParser.parse(options, args);
      if (cli.hasOption("sender_host")) {
        subs = new SubscriptionSession(cli.getOptionValue("sender_host"), PORT);
        srcHost = cli.getOptionValue("sender_host");
        out.println("sender_host:" + srcHost);
      }
      if (cli.hasOption("pattern_type")) {
        patternType = cli.getOptionValue("pattern_type");
        out.println("patternType:" + patternType);
      }
      if (cli.hasOption("topic_count")) {
        topicCount = Integer.parseInt(cli.getOptionValue("topic_count"));
        out.println("topic_count:" + topicCount);
      }
      if (cli.hasOption("databases")) {
        databases = Integer.parseInt(cli.getOptionValue("databases"));
        out.println("databases:" + databases);
      }
      if (cli.hasOption("devices")) {
        devices = Integer.parseInt(cli.getOptionValue("devices"));
        out.println("devices:" + devices);
      }
      if (cli.hasOption("timeseries")) {
        timeseries = Integer.parseInt(cli.getOptionValue("timeseries"));
        out.println("timeseries:" + timeseries);
      }
      if (cli.hasOption("format")) {
        format = cli.getOptionValue("format");
        out.println("format:" + format);
      }
      if (cli.hasOption("clean")) {
        doClean = true;
        out.println("doClean:" + doClean);
        noCreate = true;
      }
      if (cli.hasOption("receiver_host")) {
        targetHost = cli.getOptionValue("receiver_host");
        out.println("targetHost:" + targetHost);
        pool = new SessionPool(targetHost, PORT, "root", "root", 10000);
      }
      if (cli.hasOption("target_dir")) {
        targetDir = cli.getOptionValue("target_dir");
        out.println("targetDir:" + targetDir);
      }
      if (cli.hasOption("group_count")) {
        groupCount = Integer.parseInt(cli.getOptionValue("group_count"));
        out.println("group_count:" + groupCount);
      }
      if (cli.hasOption("consumer_count")) {
        consumerCount = Integer.parseInt(cli.getOptionValue("consumer_count"));
        out.println("consumer_count:" + consumerCount);
      }
      if (cli.hasOption("consumer_type")) {
        consumerType = cli.getOptionValue("consumer_type");
        out.println("consumer_type:" + consumerType);
      }

      if (cli.hasOption("no_create")) {
        noCreate = true;
        out.println("do not create topic");
      }

    } catch (ParseException e) {
      // 解析失败是用 HelpFormatter 打印 帮助信息
      helpFormatter.printHelp(">>>>>> test cli options", options);
      e.printStackTrace();
      exit(1);
    }

    subs.open();
    createTopics();
    createConsumers();
    if (doClean) {
      out.println(dateFormat.format(new Date()) + "#### drop topics ####");
      for (Topic topic : subs.getTopics()) {
        subs.dropTopicIfExists(topic.getTopicName());
      }
      out.println("###### After clean #######");
      subs.getSubscriptions().forEach(out::println);
      subs.getTopics().forEach(out::println);
      subs.close();
      out.println("###### Done #######");
    } else {
      if ("pull".equals(consumerType)) {
        out.println(dateFormat.format(new Date()) + "#### subscribe start ####");
        if (consumerCount > topicCount) {
          for (int i = 0; i < consumerCount; i++) {
            pullConsumers.get(i).subscribe(topicNamePrefix + i % topicCount);
          }
        } else {
          for (int i = 0; i < topicCount; i++) {
            pullConsumers.get(i % consumerCount).subscribe(topicNamePrefix + i);
          }
        }
        subs.getSubscriptions().forEach(out::println);
        out.println(dateFormat.format(new Date()) + "#### consume start ####");
        ExecutorService executor = Executors.newFixedThreadPool(consumerCount);
        if (TopicConstant.FORMAT_SESSION_DATA_SETS_HANDLER_VALUE.equals(format)) {
          for (int i = 0; i < consumerCount; i++) {
            SubscriptionPullConsumer consumer = pullConsumers.get(i);
            executor.submit(
                new Runnable() {
                  @Override
                  public void run() {
                    while (true) {
                      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
                      // if (messages.isEmpty()) break;
                      // consumer.commitSync(messages);
                      for (final SubscriptionMessage message : messages) {
                        for (final Iterator<Tablet> it =
                                message.getSessionDataSetsHandler().tabletIterator();
                            it.hasNext(); ) {
                          final Tablet tablet = it.next();
                          try {
                            pool.insertTablet(tablet);
                          } catch (Exception e) {
                            e.printStackTrace();
                          }
                          // for debug
                          //                            final SubscriptionSessionDataSet dataSet =
                          // new SubscriptionSessionDataSet(tablet);
                          //                            long minTime=Long.MAX_VALUE;
                          //                            long maxTime=Long.MIN_VALUE;
                          //                            while(dataSet.hasNext()) {
                          //                                final RowRecord rowRecord =
                          // dataSet.next();
                          //                                minTime = Math.min(minTime,
                          // rowRecord.getTimestamp());
                          //                                maxTime = Math.max(maxTime,
                          // rowRecord.getTimestamp());
                          //                            }
                          //                            out.printf("###### consume data with column
                          // names %s and time range [%s,%s]\n", dataSet.getColumnNames(),minTime,
                          // maxTime);
                          //                            out.println("####### consume data
                          // rowCount="+tablet.rowSize);
                        }
                      }
                    }
                  }
                });
          }
        } else {
          for (int i = 0; i < consumerCount; i++) {
            SubscriptionPullConsumer consumer = pullConsumers.get(i);
            executor.submit(
                new Runnable() {
                  @Override
                  public void run() {
                    while (true) {
                      List<SubscriptionMessage> messages = consumer.poll(Duration.ofMillis(10000));
                      for (final SubscriptionMessage message : messages) {
                        SubscriptionTsFileHandler fp = message.getTsFileHandler();
                        try {
                          fp.moveFile(
                              Paths.get(
                                  targetDir,
                                  consumer.getConsumerId()
                                      + "-"
                                      + fp.getPath().getFileName().toString()));
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }
                    }
                  }
                });
          }
        }
        executor.shutdown();
        while (true) {
          if (executor.isTerminated()) {
            System.out.println("多线程消费完成");
            break;
          }
        }
      } else {
        out.println(dateFormat.format(new Date()) + "#### subscribe start ####");
        if (consumerCount > topicCount) {
          for (int i = 0; i < consumerCount; i++) {
            pushConsumers.get(i).subscribe(topicNamePrefix + i % topicCount);
          }
        } else {
          for (int i = 0; i < topicCount; i++) {
            pushConsumers.get(i % consumerCount).subscribe(topicNamePrefix + i);
          }
        }
        subs.getSubscriptions().forEach(out::println);
        out.println(dateFormat.format(new Date()) + "#### consume start ####");
        while (true) {}
      }
    }
  }
}
