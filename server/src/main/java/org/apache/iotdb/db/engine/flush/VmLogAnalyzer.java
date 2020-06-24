package org.apache.iotdb.db.engine.flush;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.tsfile.utils.Pair;

public class VmLogAnalyzer {

  static final String STR_DEVICE_OFFSET_SEPERATOR = " ";

  private File logFile;

  public VmLogAnalyzer(File logFile) {
    this.logFile = logFile;
  }

  /**
   * @return (written device set, last offset)
   */
  public Pair<Set<String>, Long> analyze() throws IOException {
    Set<String> deviceSet = new HashSet<>();
    long offset = 0;
    String currLine;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      currLine = bufferedReader.readLine();
      if (currLine != null) {
        String[] resultList = currLine.split(STR_DEVICE_OFFSET_SEPERATOR);
        deviceSet.add(resultList[0]);
        offset = Long.parseLong(resultList[1]);
      }
    }
    return new Pair<>(deviceSet, offset);
  }
}
