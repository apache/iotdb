package org.apache.iotdb.tool;

import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.pipe.external.kafka.KafkaLoader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractKafkaTool {

  static final PrintStream console = System.out;
  static final String LOG_DIR = "./logs/LoadKafka.log";
  static final String PAUSE = "pause";
  static final String RESUME = "resume";
  static final String HELP = "help";
  static final String STATUS = "status";
  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";
  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";
  static final String USERNAME_ARGS = "u";
  static final String USERNAME_NAME = "username";
  static final String PASSWORD_ARGS = "pw";
  static final String PASSWORD_NAME = "password";
  static final String BROKERS_ARGS = "b";
  static final String BROKERS_NAME = "brokers";
  static final String TOPIC_ARGS = "t";
  static final String TOPIC_NAME = "topic";
  static final String OFFSET_ARGS = "o";
  static final String OFFSET_NAME = "offset";
  static final String MAX_CONSUMER_ARGS = "m";
  static final String MAX_CONSUMER_NAME = "max consumer num";
  static final int MAX_HELP_CONSOLE_WIDTH = 88;
  static final int CODE_ERROR = 1;
  static final int CODE_OK = 0;
  static final String SCRIPT_HINT = "./load-from-kafka.sh(load-from-kafka.bat if Windows)";
  static final String HELP_ARGS = "help";
  static final Object LOADER_CLI_PREFIX = "Loader";
  private static final String QUIT_COMMAND = "quit";
  private static final String EXIT_COMMAND = "exit";

  static Set<String> keywordSet = new HashSet<>();

  static String host = "127.0.0.1";
  static String port = "6667";
  static String username;
  static String password;

  static String brokers = "localhost:9092";
  static String topic = "IoTDB";
  static String offset = "earliest";
  static String max_consumer = "20";

  static Options createOptions() {
    Options options = new Options();

    Option help = new Option(HELP_ARGS, false, "Display help information(optional)");
    help.setRequired(false);
    options.addOption(help);

    Option host =
        Option.builder(HOST_ARGS)
            .argName(HOST_NAME)
            .hasArg()
            .desc("Host Name (optional, default 127.0.0.1)")
            .build();
    options.addOption(host);

    Option port =
        Option.builder(PORT_ARGS)
            .argName(PORT_NAME)
            .hasArg()
            .desc("Port (optional, default 6667)")
            .build();
    options.addOption(port);

    Option username =
        Option.builder(USERNAME_ARGS)
            .argName(USERNAME_NAME)
            .hasArg()
            .desc("User name (required)")
            .required()
            .build();
    options.addOption(username);

    Option password =
        Option.builder(PASSWORD_ARGS)
            .argName(PASSWORD_NAME)
            .hasArg()
            .desc("password (optional)")
            .build();
    options.addOption(password);

    Option brokers =
        Option.builder(BROKERS_ARGS)
            .argName(BROKERS_NAME)
            .hasArg()
            .desc("brokers (optional, default localhost:9092)")
            .build();
    options.addOption(brokers);

    Option topic =
        Option.builder(TOPIC_ARGS)
            .argName(TOPIC_NAME)
            .hasArg()
            .desc("topic (optional, default IoTDB)")
            .build();
    options.addOption(topic);

    Option offset =
        Option.builder(OFFSET_ARGS)
            .argName(OFFSET_NAME)
            .hasArg()
            .desc("offset (optional, default earliest)")
            .build();
    options.addOption(offset);

    Option max_consumer =
        Option.builder(MAX_CONSUMER_ARGS)
            .argName(MAX_CONSUMER_NAME)
            .hasArg()
            .desc("Max number of consumer (optional, default 20)")
            .build();
    options.addOption(max_consumer);

    return options;
  }

  static void init() {
    keywordSet.add("-" + HOST_ARGS);
    keywordSet.add("-" + HELP_ARGS);
    keywordSet.add("-" + PORT_ARGS);
    keywordSet.add("-" + PASSWORD_ARGS);
    keywordSet.add("-" + USERNAME_ARGS);
    keywordSet.add("-" + BROKERS_ARGS);
    keywordSet.add("-" + TOPIC_ARGS);
    keywordSet.add("-" + OFFSET_ARGS);
    keywordSet.add("-" + MAX_CONSUMER_ARGS);
  }

  static String[] removePasswordArgs(String[] args) {
    int index = -1;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-" + PASSWORD_ARGS)) {
        index = i;
        break;
      }
    }
    if (index >= 0
        && ((index + 1 >= args.length)
            || (index + 1 < args.length && keywordSet.contains(args[index + 1])))) {
      return ArrayUtils.remove(args, index);
    }
    return args;
  }

  static String checkRequiredArg(
      String arg, String name, CommandLine commandLine, boolean isRequired, String defaultValue)
      throws ArgsErrorException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      if (isRequired) {
        String msg =
            String.format(
                "%s: Required values for option '%s' not provided", LOADER_CLI_PREFIX, name);
        console.println(msg);
        console.println("Use -help for more information");
        throw new ArgsErrorException(msg);
      } else if (defaultValue == null) {
        String msg =
            String.format("%s: Required values for option '%s' is null.", LOADER_CLI_PREFIX, name);
        throw new ArgsErrorException(msg);
      } else {
        return defaultValue;
      }
    }
    return str;
  }

  static boolean handleInputCmd(String cmd, KafkaLoader kl) {
    String specialCmd = cmd.toLowerCase().trim();
    if (QUIT_COMMAND.equals(specialCmd) || EXIT_COMMAND.equals(specialCmd)) {
      kl.close();
      return false;
    }
    if (HELP.equals(specialCmd)) {
      showHelp();
      return true;
    }
    if (PAUSE.equals(specialCmd)) {
      kl.cancel();
      console.println("The statement has been executed successfully.");
      return true;
    }
    if (RESUME.equals(specialCmd)) {
      kl.run();
      console.println("The statement has been executed successfully.");
      return true;
    }
    if (STATUS.equals(specialCmd)) {
      console.println(kl.getStatus());
      console.println(kl);
      return true;
    }
    console.println("Error: invalid command type. Correct commands shall be:");
    showHelp();
    return true;
  }

  private static void showHelp() {
    console.printf("    %s:\t\t\t pause the synchronization process%n", PAUSE);
    console.printf("    %s:\t\t\t restart the synchronization process%n", RESUME);
    console.printf("    %s:\t\t\t show the status of the loader%n", STATUS);
    console.printf("    %s or %s:\t quit the kafka loader tool.%n", QUIT_COMMAND, EXIT_COMMAND);
  }

  static boolean processCommand(String s, KafkaLoader kl) {
    if (s == null) {
      return true;
    }
    String[] cmds = s.trim().split(";");
    for (String cmd : cmds) {
      if (cmd != null && !"".equals(cmd.trim())) {
        boolean result = handleInputCmd(cmd, kl);
        if (!result) {
          return false;
        }
      }
    }
    return true;
  }

  public static void createFile(File file) throws IOException {
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    file.createNewFile();
  }
}
