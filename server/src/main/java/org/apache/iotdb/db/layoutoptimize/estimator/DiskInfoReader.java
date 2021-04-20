package org.apache.iotdb.db.layoutoptimize.estimator;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class DiskInfoReader {
  File csvFile;
  Scanner scanner;

  public DiskInfoReader(File file) throws IOException {
    csvFile = file;
    scanner = new Scanner(csvFile);
  }

  public String[] getNextSeekData() throws IOException {
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

  public double getReadSpeed() throws IOException {
    if (!scanner.hasNext()) {
      throw new IOException("Out of file range");
    }
    return Double.parseDouble(scanner.nextLine());
  }

  public boolean hasNext() {
    return scanner.hasNext();
  }
}
