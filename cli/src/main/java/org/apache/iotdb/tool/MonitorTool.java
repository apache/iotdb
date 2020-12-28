package org.apache.iotdb.tool;

import com.sun.management.OperatingSystemMXBean;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.NetworkInterface;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.util.Enumeration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public class MonitorTool {

  private static String url;

  public static void main(String[] args) throws ParseException, IoTDBConnectionException {
    int port = 6667;
    String password = "root";
    Options options = new Options();
    Option opt_help = new Option("help", "help", false, "print help message");
    opt_help.setRequired(false);
    options.addOption(opt_help);

    Option opt_port = new Option("p", "port", true, "port");
    opt_port.setRequired(false);
    options.addOption(opt_port);

    Option opt_password = new Option("pw", "password", true, "password");
    opt_password.setRequired(false);
    options.addOption(opt_password);

    HelpFormatter hf=new HelpFormatter();
    hf.setWidth(110);

    CommandLine commandLine;
    CommandLineParser parser=new DefaultParser();

    commandLine = parser.parse(options,args);

    if(commandLine.hasOption("help")){
      hf.printHelp("correctionTest",options,true);
    } else {
      if(commandLine.hasOption("p")) {
        port = Integer.parseInt(commandLine.getOptionValue("p"));
      }
      if(commandLine.hasOption("pw")) {
        password = commandLine.getOptionValue("pw");
      }
      Session session = new Session("127.0.0.1", port, "root", password);
      session.open();

      url = System.getProperty("IOTDB_HOME", null);

      if(url == null) {
        url = MonitorTool.class.getResource("/").toString();
      }
    }

  }

  public static void generateOsFile() throws IOException {

    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(url + "/os.txt")));
    //operate system name
    String osName = System.getProperty("os.name");
    writer.write("operate system: " + osName + "\n");

    //jvm memory size in mb
    long freeMemory = Runtime.getRuntime().freeMemory() / (1024 * 1024);
    long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
    long totalMemory = Runtime.getRuntime().totalMemory() / (1024 * 1024);
    long usedMemory = totalMemory - freeMemory;
    writer.write("jvm max memory: " + maxMemory + "mb"+ "\n");
    writer.write("jvm total memory: " + totalMemory + "mb" + "\n");
    writer.write("jvm used memory: " + usedMemory + "mb" + "\n");
    writer.write("jvm free memory: " + freeMemory + "mb" + "\n");

    // underlying file stores size in mb
    for (FileStore store : FileSystems.getDefault().getFileStores()) {
      writer.write("file store name: " + store.name());
      long total = store.getTotalSpace() / (1024 * 1024);
      long used = (store.getTotalSpace() - store.getUnallocatedSpace()) / (1024 * 1024);
      long avail = store.getUsableSpace() / (1024 * 1024);
      writer.write("---- " + store.name() + "'s total space: " + total + "mb" + "\n");
      writer.write("---- " + store.name() + "'s used space: " + used + "mb" + "\n");
      writer.write("---- " + store.name() + "'s avail space: " + avail + "mb" + "\n");
    }

    //network interface name
    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();

    writer.write("network interface name: " + "\n");
    while (interfaces.hasMoreElements())
    {
      NetworkInterface networkInterface = interfaces.nextElement();
      String interfacesName = networkInterface.getDisplayName();
      writer.write("---- " + interfacesName + "\n");
    }

    //jdk version
    String jdk = System.getProperty("java.version");
    writer.write("jdk version: " + jdk + "\n");

    //operate system memory size in mb
    OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
        OperatingSystemMXBean.class);

    long freeSystemMemory = osBean.getFreePhysicalMemorySize() / (1024 * 1024);
    long totalSystemMemory = osBean.getTotalPhysicalMemorySize() / (1024 * 1024);
    long usedSystemMemory = totalSystemMemory - freeSystemMemory / (1024 * 1024);

    writer.write("free system memory: " + freeSystemMemory);
    writer.write("total system memory: " + totalSystemMemory);
    writer.write("used system memory: " + usedSystemMemory);

    writer.write("Thread info: ");
    for(Thread thread : Thread.getAllStackTraces().keySet()) {
      writer.write("---- " + thread.getName());
    }

    writer.close();
  }

  public static void generateIoTDBRuntimeFile(int port, String password)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(url + "/iotdb.txt")));
    Session session = new Session("127.0.0.1", port, "root", password);
    session.open();
    SessionDataSet dataSet = session.executeQueryStatement("count timeseries root");
    RowRecord record = dataSet.next();
    writer.write("number of sensors: " + record.getFields().get(0).getLongV() + "\n");
    dataSet = session.executeQueryStatement("count devices root");
    record = dataSet.next();
    writer.write("number of devices: " + record.getFields().get(0).getLongV() + "\n");
    dataSet = session.executeQueryStatement("count storage group root");
    record = dataSet.next();
    writer.write("number of storage group: " + record.getFields().get(0).getLongV() + "\n");
    dataSet = session.executeQueryStatement("count clients");
    record = dataSet.next();
    writer.write("number of clients: " + record.getFields().get(0).getLongV() + "\n");
    dataSet = session.executeQueryStatement("show configuration");
    while(dataSet.hasNext()) {
      record = dataSet.next();
      writer.write(record.getFields().get(0).getBinaryV().toString() + ": ");
      writer.write(record.getFields().get(1).getBinaryV().toString() + "\n");
    }
  }
}
