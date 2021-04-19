package org.apache.iotdb.db.layoutoptimize.estimator;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class CsvReader {
  File csvFile;
  Scanner scanner;

  public CsvReader(File file) throws IOException {
    csvFile = file;
    scanner = new Scanner(csvFile);
  }

  public String[] readNext() throws IOException {
    if (!scanner.hasNext()) return null;
    String line = scanner.nextLine();
    String[] splitLine = null;
    if (line.indexOf(',') != -1) {
      splitLine = line.split(",");
    } else if (line.indexOf(' ') != -1) {
      splitLine = line.split(" ");
    }
    return splitLine;
  }

  public boolean hasNext() {
    return scanner.hasNext();
  }
}
