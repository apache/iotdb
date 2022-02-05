package org.apache.iotdb.queryExp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class SumResultUnify {

  public static void main(String[] args) throws IOException {
    String moc = args[0]; // sumResultMOC.csv
    String mac = args[1]; // sumResultMAC.csv
    String cpv = args[2]; // sumResultCPV.csv
    String out = args[3];

    BufferedReader mocReader = new BufferedReader(new FileReader(moc));
    BufferedReader macReader = new BufferedReader(new FileReader(mac));
    BufferedReader cpvReader = new BufferedReader(new FileReader(cpv));
    PrintWriter printWriter = new PrintWriter(new FileWriter(out));
    String mocLine;
    String macLine;
    String cpvLine;
    String appendLine;
    while ((mocLine = mocReader.readLine()) != null) {
      macLine = macReader.readLine();
      cpvLine = cpvReader.readLine();
      appendLine = mocLine + "," + macLine + "," + cpvLine;
      printWriter.println(appendLine);
    }
    printWriter.close();
    mocReader.close();
    macReader.close();
    cpvReader.close();
  }
}
