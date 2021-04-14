package org.apache.iotdb.db.layoutoptimize.diskevaluate;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;

public class DiskEvaluator {
  private static final DiskEvaluator INSTANCE = new DiskEvaluator();
  private static final Logger logger = LoggerFactory.getLogger(DiskEvaluator.class);
  private final String sudoPassword = "601tif";

  public static DiskEvaluator getInstance() {
    return INSTANCE;
  }

  private DiskEvaluator() {}

  /**
   * Create a temporary file foreek test
   *
   * @param fileSize the size of the created file
   * @param dataPath the path of the created file
   * @return return the instance of the created file
   * @throws IOException throw the IOException if the file already exists or cannot create it
   */
  public File generateFile(final long fileSize, String dataPath, int fileNum) throws IOException {
    File file = new File(dataPath);
    if (file.exists()) {
      throw new IOException(String.format("%s already exists", dataPath));
    }
    // 1 MB buffer size
    final int bufferSize = 1 * 1024 * 1024;
    byte[] buffer = new byte[bufferSize];
    buffer[0] = 1;
    buffer[1] = 2;
    buffer[2] = 3;

    // number of block to write
    int blockNum = (int) (fileSize / bufferSize);

    if (!file.mkdirs()) {
      throw new IOException(String.format("failed to create %s", dataPath));
    }
    for (int i = 0; i < fileNum; ++i) {
      logger.info(String.format("Creating file%d", i));
      BufferedOutputStream os =
          new BufferedOutputStream(new FileOutputStream(file.getPath() + "/" + i));
      long startTime = System.nanoTime();
      for (int j = 0; j < blockNum; ++j) {
        os.write(buffer);
      }
      os.flush();
      os.close();
      long endTime = System.nanoTime();
      double lastTime = ((double) (endTime - startTime)) / 1000 / 1000;
      double writeSpeed = ((double) fileSize) / lastTime / 1024;
      System.out.println(
          String.format("Write %d KB in %.2f s, %.2f MB/s", fileSize / 1024, lastTime, writeSpeed));
    }
    return file;
  }

  /**
   * perform the seek test on the given file
   *
   * @param seekCosts the array to record the seek cost,tt is millisecond
   * @param file the file to perform seek test
   * @param numSeeks the number of seek that needs to be performed
   * @param readLength the length of data to be read after each seek
   * @param seekDistance the distance of each seek
   * @param interval the interval between each seek
   * @return -1 if the seek doesn't perform, else the number of performed seeks
   * @throws IOException throw the IOException if the file doesn't exist
   */
  public int performSeek(
      long[] seekCosts,
      final File file,
      final int numSeeks,
      final int readLength,
      final long seekDistance,
      final long interval)
      throws IOException {
    if (seekCosts.length < numSeeks) {
      return -1;
    }
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    int readSize = 0;
    byte[] buffer = new byte[readLength];

    long fileLen = file.length();
    long curPos = 0;
    raf.seek(curPos);
    while (readSize < readLength) {
      readSize += raf.read(buffer, readSize, readLength - readSize);
    }
    curPos += seekDistance;

    int i = 0;
    for (; i < numSeeks && curPos < fileLen; ++i) {
      readSize = 0;
      CmdExecutor.builder(sudoPassword)
          .errRedirect(false)
          .verbose(false)
          .sudoCmd("echo 3 | tee /proc/sys/vm/drop_caches")
          .exec();
      long startTime = System.nanoTime();
      raf.seek(curPos);
      while (readSize < readLength) {
        readSize += raf.read(buffer, readSize, readLength - readSize);
      }
      curPos += interval;
      long endTime = System.nanoTime();
      seekCosts[i] = (endTime - startTime) / 1000;
    }

    return i;
  }

  /**
   * perform the read test on the given file, to get the read speed of the disk
   *
   * @param file the file to be test
   * @return the read speed of the disk in KB/s
   * @throws IOException throw the IOException if the file doesn't exist
   */
  public double performRead(final File file) throws IOException {
    // clean the caches
    CmdExecutor.builder(sudoPassword).sudoCmd("echo 3 | tee /proc/sys/vm/drop_caches");

    long dataSize = file.length();
    // 1MB buffer size
    final int bufferSize = 1 * 1024 * 1024;
    byte[] buffer = new byte[bufferSize];
    long readSize = 0;

    RandomAccessFile raf = new RandomAccessFile(file, "r");
    raf.seek(0);

    long startTime = System.nanoTime();
    while (readSize < dataSize) {
      readSize += raf.read(buffer, 0, bufferSize);
    }
    long endTime = System.nanoTime();
    // read time in seconds
    double readTime = ((double) (endTime - startTime)) / 1000 / 1000 / 1000;
    // read speed in KB
    double readSpeed = ((double) dataSize) / readTime / 1024;
    System.out.println(
        String.format(
            "Read %d KB in %.2f seconds, %.2f KB/s", dataSize / 1024, readTime, readSpeed));

    return readSpeed;
  }

  private double getAvgCost(long[] costs, int num) {
    if (num == -1) {
      return -1.0d;
    }
    BigDecimal totalCost = BigDecimal.valueOf(0);
    for (int i = 0; i < num; ++i) {
      totalCost = totalCost.add(BigDecimal.valueOf(costs[i]));
    }
    return totalCost.divide(BigDecimal.valueOf(costs.length)).doubleValue();
  }

  public void performMultiSegmentSeek(
      final long seekDistance,
      final long seekNum,
      final int seekNumForPerSegment,
      File file,
      final int readLength,
      double[] seekResult)
      throws IOException {
    File[] files = InputFactory.Instance().getFiles(file.getPath());
    final String logPath = IoTDBDescriptor.getInstance().getConfig().getSystemDir() + "/seek.txt";
    final File logFile = new File(logPath);
    if (!logFile.exists()) {
      logFile.createNewFile();
    }
    BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile));
    for (int j = 1; j <= seekNum; ++j) {
      logger.info(String.format("Seeking test %.2f%%", ((double) j) / seekNum * 100.0d));
      long curSeekDistance = seekDistance * j;

      // drop caches before seeks
      CmdExecutor.builder(sudoPassword)
          .errRedirect(false)
          .verbose(false)
          .sudoCmd("echo 3 | sudo tee /proc/sys/vm/drop_caches");

      double totalSeekCost = 0;
      for (int i = 0; i < files.length; i += 2) {
        File curFile = files[i];
        long[] seekCosts = new long[seekNumForPerSegment];
        int realNumSeeks =
            performLocalSeeks(
                seekCosts, curFile, seekNumForPerSegment, readLength, curSeekDistance);
        double avgCost = getAvgCost(seekCosts, realNumSeeks);
        totalSeekCost += avgCost / 1000;
      }
      double realAvgCost = totalSeekCost / ((files.length) / 2);
      logWriter.write(
          String.format("Seek distance: %d byte, %.3f ms\n", curSeekDistance, realAvgCost));
      seekResult[j - 1] = realAvgCost;
    }
    logWriter.flush();
    logWriter.close();
  }

  public int performLocalSeeks(
      long[] seekCosts,
      final File file,
      final int numSeeks,
      final int readLength,
      final long seekDistance)
      throws IOException {
    if (seekCosts.length < numSeeks) {
      return -1;
    }
    long fileLen = file.length();
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    int readSize = 0;
    byte[] buffer = new byte[readLength];

    long pos = 0;
    raf.seek(pos);
    while (readSize < readLength) {
      readSize += raf.read(buffer, readSize, readLength - readSize);
    }
    pos += seekDistance;

    int i = 0;
    for (; i < numSeeks && pos < fileLen; ++i) {
      readSize = 0;
      long startNanoTime = System.nanoTime();
      raf.seek(pos);
      while (readSize < readLength) {
        readSize += raf.read(buffer, readSize, readLength - readSize);
      }
      long endNanoTime = System.nanoTime();
      seekCosts[i] = (endNanoTime - startNanoTime) / 1000;
      pos += seekDistance;
    }
    raf.close();

    return i;
  }

  public void performLocalSeek(
      String dataPath, long seekDistInterval, int numInvervals, int numSeeks, int readLength) {
    try {
      File[] files = InputFactory.Instance().getFiles(dataPath);
      BufferedWriter seekCostWriter =
          new BufferedWriter(
              new FileWriter(
                  IoTDBDescriptor.getInstance().getConfig().getSystemDir() + "/seek_cost.txt"));
      seekCostWriter.write(seekDistInterval + "\n");
      seekCostWriter.write("0\t0\n");

      for (int j = 1; j <= numInvervals; ++j) {
        long seekDistance = seekDistInterval * j;

        // drop caches before seeks.
        CmdExecutor.builder("tif601")
            .errRedirect(false)
            .sudoCmd("echo 3 | sudo tee /proc/sys/vm/drop_caches")
            .exec();

        double totalSeekCost = 0;

        for (int i = 0; i < files.length; i += 2) {
          // get the local file
          File file = files[i];
          long[] seekCosts = new long[numSeeks];
          int realNumSeeks = performLocalSeeks(seekCosts, file, numSeeks, readLength, seekDistance);
          double fileMedCost = getAvgCost(seekCosts, realNumSeeks);
          totalSeekCost += fileMedCost / 1000;
        }

        double avgSeekCost = totalSeekCost / ((files.length) / 2);
        // seek cost in seek_cost.txt is in ms
        seekCostWriter.write(seekDistance + "\t" + avgSeekCost + "\n");
        seekCostWriter.flush();
      }
      seekCostWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    DiskEvaluator diskEvaluator = DiskEvaluator.getInstance();
    String path = "/data/tmp";
    File file = new File(path);
    try {
      if (!file.exists()) {
        file = diskEvaluator.generateFile(4l * 1024l * 1024l * 1024l, path, 20);
      }
      //      double[] seekTime = new double[100];
      //      diskEvaluator.performLocalSeek(path, 512, 1000, 100, 100);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
