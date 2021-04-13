package org.apache.iotdb.db.layoutoptimize;

import java.io.*;

public class DiskEvaluator {
	private static final DiskEvaluator INSTANCE = new DiskEvaluator();

	public static DiskEvaluator getInstance() {
		return INSTANCE;
	}

	private DiskEvaluator() {
	}

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
		for (int i = 0; i < blockNum; ++i) {
			os.write(buffer);
		}
		os.flush();
		os.close();
		return file;
	}

	/**
	 * perform the seek test on the given file
	 * @param seekCosts the array to record the seek cost,tt is millisecond
	 * @param file the file to perform seek test
	 * @param numSeeks the number of seek that needs to be performed
	 * @param readLength the length of data to be read after each seek
	 * @param seekDistance the distance of each seek
	 * @return -1 if the seek doesn't perform, else the number of performed seeks
	 * @throws IOException throw the IOException if the file doesn't exist
	 */
	public int performSeek(long[] seekCosts, final File file,
												 final int numSeeks, final int readLength, final long seekDistance) throws IOException {
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
			curPos += seekDistance;
			long endTime = System.nanoTime();
			seekCosts[i] = (endTime - startTime) / 1000;
		}

		return i;
	}

	public static void main(String[] args) throws Exception {
		DiskEvaluator evaluator = DiskEvaluator.getInstance();
		evaluator.generateFile(1024 * 1024, "./test.tmp");
	}
}
