package org.apache.iotdb.pipe.external.kafka;

import org.apache.commons.cli.CommandLine;
import org.jline.reader.LineReader;

public class KafkaSyncCli {
  static final String PAUSE = "pause";
  static final String RESUME = "resume";
  static final String HELP = "help";
  static final String EXIT = "exit";

  private static CommandLine commandLine;
  private static LineReader lineReader;
}
