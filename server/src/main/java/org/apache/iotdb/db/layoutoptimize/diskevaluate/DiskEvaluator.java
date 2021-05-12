package org.apache.iotdb.db.layoutoptimize.diskevaluate;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DiskEvaluator {
  private static final Logger logger = LoggerFactory.getLogger(DiskEvaluator.class);
  private static final DiskEvaluator INSTANCE = new DiskEvaluator();
  private final String sudoPassword = "601tif";
  public int GENERATE_FILE_NUM = 20;
  public long GENERATE_FILE_SIZE = 8l * 1024l * 1024l * 1024l; // 8 GB
  public long SEEK_INTERVAL = 1024;
  public int SEEK_NUM = 5000;
  public int SEEK_NUM_PER_SEGMENT = 50;
  public int READ_LENGTH = 100;
  private DiskInfo diskInfo = new DiskInfo();

  public static DiskEvaluator getInstance() {
    return INSTANCE;
  }

  private DiskEvaluator() {
    recoverFromFile();
  }

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
      double lastTime = ((double) (endTime - startTime)) / 1000 / 1000 / 1000;
      double writeSpeed = ((double) fileSize) / lastTime / 1024 / 1024;
      System.out.println(
          String.format("Write %d KB in %.2f s, %.2f MB/s", fileSize / 1024, lastTime, writeSpeed));
    }
    return file;
  }

  /**
   * perform the read test on several file to get the long-term read performance of the disk
   *
   * @param directory the directory containing the test file
   * @return the read speed of the disk in Byte/ms
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
    BigInteger totalSize = BigInteger.valueOf(0);
    long totalTime = 0;
    for (File file : files) {
      long dataSize = file.length();
      totalSize = totalSize.add(BigInteger.valueOf(dataSize));
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
    // read speed in Byte/ms
    double readSpeed = totalSize.divide(BigInteger.valueOf(totalTime)).doubleValue();
    logger.info(
        String.format(
            "Read %s MB in %.2f seconds, %.2f MB/s",
            totalSize.divide(BigInteger.valueOf(1024 * 1024)).toString(),
            (double) totalTime / 1000.0d,
            readSpeed * 1000 / 1024 / 1024));
    diskInfo.setReadSpeed(readSpeed);
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

      int oneTenthIntervals = numIntervals / 10;
      for (int j = 1; j <= numIntervals; ++j) {
        if (j % oneTenthIntervals == 0 || j == 1) {
          logger.info("seeking for {} interval, {} intervals in total", j, numIntervals);
        }
        long seekDistance = seekDistInterval * j;

        // drop caches before seeks.
        CmdExecutor.builder(sudoPassword)
            .verbose(false)
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
        diskInfo.addSeekInfo(seekDistance, avgSeekCost);
      }
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
    logger.info("evaluating disk...");
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    DiskInfo[] diskInfos = new DiskInfo[dataDirs.length];
    for (int i = 0; i < dataDirs.length; ++i) {
      String dataDir = dataDirs[i];
      diskInfo = new DiskInfo();
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
        performRead(tmpDir);
      }
      for (File file : files) {
        file.delete();
      }
      tmpDir.delete();
      diskInfos[i] = diskInfo;
    }
    this.diskInfo = DiskInfo.calAvgInfo(diskInfos);
    // persist in json format
    Gson gson = new Gson();
    String json = gson.toJson(diskInfo);
    String systemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    File layoutDir = new File(systemDir + File.separator + "layout");
    if (!layoutDir.exists()) {
      layoutDir.mkdir();
    }
    File diskInfoFile = new File(layoutDir + File.separator + "disk.info");
    if (!diskInfoFile.exists()) {
      diskInfoFile.createNewFile();
    }
    BufferedOutputStream outputStream =
        new BufferedOutputStream(new FileOutputStream(diskInfoFile));
    outputStream.write(json.getBytes(StandardCharsets.UTF_8));
    outputStream.flush();
    outputStream.close();
  }

  public boolean recoverFromFile() {
    File layoutDir = new File(IoTDBDescriptor.getInstance().getConfig().getLayoutDir());
    File diskInfoFile = new File(layoutDir + File.separator + "disk.info");
    if (!diskInfoFile.exists()) {
      logger.info("failed to recover from file, because {} not exist", diskInfoFile);
      return false;
    }
    try {
      BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(diskInfoFile));
      byte[] buffer = new byte[(int) diskInfoFile.length()];
      inputStream.read(buffer);
      String json = new String(buffer);
      Gson gson = new Gson();
      diskInfo = gson.fromJson(json, DiskInfo.class);
    } catch (IOException e) {
      e.printStackTrace();
      diskInfo = null;
      return false;
    }
    return true;
  }

  public DiskInfo getDiskInfo() {
    if (diskInfo.seekCost.size() == 0) {
      recoverFromFile();
    }
    return diskInfo;
  }

  public static class DiskInfo {
    public List<Long> seekDistance = new ArrayList<>();
    public List<Double> seekCost = new ArrayList<>();
    public double readSpeed = 0;

    public void addSeekInfo(long distance, double cost) {
      seekDistance.add(distance);
      seekCost.add(cost);
    }

    public void setReadSpeed(double readSpeed) {
      this.readSpeed = readSpeed;
    }

    public static DiskInfo calAvgInfo(DiskInfo[] infos) {
      DiskInfo avgDiskInfo = null;
      if (infos.length == 0) {
        avgDiskInfo = null;
      } else if (infos.length == 1) {
        avgDiskInfo = infos[0];
      } else {
        int totalLength = infos[0].seekCost.size();
        avgDiskInfo = new DiskInfo();
        for (int i = 0; i < totalLength; ++i) {
          BigDecimal totalCost = new BigDecimal(0);
          for (DiskInfo info : infos) {
            totalCost = totalCost.add(BigDecimal.valueOf(info.seekCost.get(i)));
          }
          avgDiskInfo.addSeekInfo(
              infos[0].seekDistance.get(i),
              totalCost.divide(BigDecimal.valueOf(infos.length)).doubleValue());
        }
      }
      return avgDiskInfo;
    }
  }
}
