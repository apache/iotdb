package org.apache.iotdb.db.layoutoptimize;

import java.io.*;

public class DiskEvaluator {
  private static final DiskEvaluator INSTANCE = new DiskEvaluator();

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
  public File generateFile(final long fileSize, String dataPath) throws IOException {
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

    if (!file.createNewFile()) {
      throw new IOException(String.format("failed to create %s", dataPath));
    }
    BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(file));
    long startTime = System.nanoTime();
    for (int i = 0; i < blockNum; ++i) {
      os.write(buffer);
    }
    os.flush();
    os.close();
    long endTime = System.nanoTime();
    double lastTime = ((double)(endTime - startTime)) / 1000 / 1000;
    double writeSpeed = ((double)fileSize) / lastTime / 1024;
    System.out.println(String.format("Write %d KB in %.2f s, %.2f MB/s", fileSize / 1024, lastTime, writeSpeed));
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
    System.out.println(String.format("Read %d KB in %.2f seconds, %.2f KB/s", dataSize / 1024, readTime, readSpeed));

    return readSpeed;
  }

  public static void main(String[] args) {
    DiskEvaluator evaluator = DiskEvaluator.getInstance();
    try {
      File generatedFile = evaluator.generateFile(1024l * 1024l * 1024l * 8l, "E:\\test.tmp");
      long[] seekCost = new long[10];
      // seek 1MB for 10 times
      if (evaluator.performSeek(seekCost, generatedFile, 10, 512, 50 * 1024, 8000l * 1024l * 1024l) != -1) {
        for (long cost : seekCost) {
          System.out.println(cost);
        }
      }
//      System.out.println(evaluator.performRead(generatedFile));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
