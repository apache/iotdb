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
  public int GENERATE_FILE_NUM = 20;
  public long GENERATE_FILE_SIZE = 8l * 1024l * 1024l * 1024l; // 8 GB
  public long SEEK_INTERVAL = 1024;
  public int SEEK_NUM = 5000;
  public int SEEK_NUM_PER_SEGMENT = 50;
  public int READ_LENGTH = 100;

  public static DiskEvaluator getInstance() {
    return INSTANCE;
  }

  private DiskEvaluator() {}

  /**
   * Create a temporary file for seek test
   *
   * @param fileSize the size of the created file
   * @param dataPath the path of the created file
   * @return return the instance of the created file
   * @throws IOException throw the IOException if the file already exists or cannot create it
   */
  public synchronized File generateFile(final long fileSize, String dataPath, int fileNum)
      throws IOException {
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
   * peform the read test on several file to get the long-term read performance of the disk
   *
   * @param directory the directory containing the test file
   * @return the read speed of the disk in KB/s
   * @throws IOException throw the IOException if the file doesn't exist
   */
  public synchronized double performRead(final File directory) throws IOException {
    // clean the caches
    CmdExecutor cleaner =
        CmdExecutor.builder(sudoPassword).sudoCmd("echo 3 | tee /proc/sys/vm/drop_caches");
    cleaner.exec();
    if (!directory.exists() || !directory.isDirectory())
      throw new IOException(
          String.format("%s does not exist or is not a directory", directory.getPath()));
    File[] files = InputFactory.Instance().getFiles(directory.getPath());
    if (files.length == 0) {
      throw new IOException(String.format("%s contains no file", directory.getPath()));
    }
    long totalSize = 0;
    long totalTime = 0;
    BufferedWriter logWriter =
        new BufferedWriter(
            new FileWriter(
                IoTDBDescriptor.getInstance().getConfig().getSystemDir()
                    + File.separator
                    + "disk.info",
                true));
    for (File file : files) {
      long dataSize = file.length();
      totalSize += dataSize;
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
      // read time in millisecond
      double readTime = (endTime - startTime) / 1000 / 1000;
      totalTime += readTime;
      cleaner.exec();
    }
    // read speed in KB/s
    double readSpeed = ((double) totalSize) / totalTime / 1000 / 1024;
    logger.info(
        String.format(
            "Read %d KB in %.2f seconds, %.2f KB/s",
            totalSize / 1024, totalTime / 1000, readSpeed));
    logWriter.write(String.format("Read speed: %.2f bytes/s\n", readSpeed));
    logWriter.flush();
    logWriter.close();
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

  /**
   * perform seek evaluation in a single given file
   *
   * @param seekCosts the array of the seek time
   * @param file the file to test on
   * @param numSeeks the number of seek
   * @param readLength the data length to be read after each seek
   * @param seekDistance the distance of each seek
   * @return if the seek doesn't perform , return -1; else return the actual time of seek
   * @throws IOException throws IOException if the file doesn't exist
   */
  public synchronized int performLocalSeekInSingleFile(
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

  /**
   * perform seek evaluation on multiple files to obtain a relatively accurate disk seek time
   *
   * @param DiskId the id of the disk, usually the directory where the data is stored
   * @param dataPath the temporary dir where store the test file
   * @param seekDistInterval the interval between each seek
   * @param numIntervals the total multiple of disk seek interval growth
   * @param numSeeks the seek time for each interval in each file
   * @param readLength the length of data after each seek
   */
  public synchronized void performLocalSeekInMultiFiles(
      String DiskId,
      String dataPath,
      long seekDistInterval,
      int numIntervals,
      int numSeeks,
      int readLength) {
    try {
      File[] files = InputFactory.Instance().getFiles(dataPath);
      BufferedWriter seekCostWriter =
          new BufferedWriter(
              new FileWriter(
                  IoTDBDescriptor.getInstance().getConfig().getSystemDir() + "/disk_info.txt",
                  true));
      seekCostWriter.write(DiskId + "\n");

      for (int j = 1; j <= numIntervals; ++j) {
        long seekDistance = seekDistInterval * j;

        // drop caches before seeks.
        CmdExecutor.builder(sudoPassword)
            .errRedirect(false)
            .sudoCmd("echo 3 | sudo tee /proc/sys/vm/drop_caches")
            .exec();

        double totalSeekCost = 0;

        for (int i = 0; i < files.length; i += 2) {
          // get the local file
          File file = files[i];
          long[] seekCosts = new long[numSeeks];
          int realNumSeeks =
              performLocalSeekInSingleFile(seekCosts, file, numSeeks, readLength, seekDistance);
          double fileMedCost = getAvgCost(seekCosts, realNumSeeks);
          totalSeekCost += fileMedCost / 1000;
        }

        double avgSeekCost = totalSeekCost / ((files.length) / 2);
        // seek cost in disk_info.txt is in ms
        seekCostWriter.write(seekDistance + "\t" + avgSeekCost + "\n");
        seekCostWriter.flush();
      }
      seekCostWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * evaluate the disk performance both in seek and read, and store the result in
   * SystemDir/disk.info
   *
   * @throws IOException throw IOException if fail to create the test file
   */
  public synchronized void performDiskEvaluation() throws IOException {
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    for (String dataDir : dataDirs) {
      String tmpDirPath = dataDir + File.separator + "seek_test";
      File tmpDir = new File(tmpDirPath);
      if (tmpDir.exists()) {
        continue;
      }
      generateFile(GENERATE_FILE_SIZE, tmpDirPath, GENERATE_FILE_NUM);
      performLocalSeekInMultiFiles(
          dataDir, tmpDirPath, SEEK_INTERVAL, SEEK_NUM, SEEK_NUM_PER_SEGMENT, READ_LENGTH);
      File[] files = tmpDir.listFiles();
      if (files.length != 0) {
        File file = files[0];
        performRead(file);
      }
      for (File file : files) {
        file.delete();
      }
      tmpDir.delete();
    }
  }
}
