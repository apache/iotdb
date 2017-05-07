package cn.edu.thu.tsfiledb.engine.overflow.io;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;

/**
 * The stream of bytes for overflow read and write
 * 
 * @author kangrong
 * @author liukun
 *
 */
public class OverflowReadWriter extends OutputStream implements TSRandomAccessFileReader, TSRandomAccessFileWriter {
	private RandomAccessFile raf;
	private final String filePath;
	private final int onePassCopySize = 4 * 1024 * 1024;
	private static final String RW_MODE = "rw";
	private static final String R_MODE = "r";

	public OverflowReadWriter(String filePath) throws IOException {
		this.raf = new RandomAccessFile(filePath, RW_MODE);
		this.filePath = filePath;
	}

	public OverflowReadWriter(File file) throws IOException {
		this.raf = new RandomAccessFile(file, RW_MODE);
		this.filePath = file.getCanonicalPath();
	}

	public String getFilePath() {
		return filePath;
	}

	@Override
	public void write(int b) throws IOException {
		raf.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		raf.write(b);
	}

	@Override
	public void write(byte b[], int off, int len) throws IOException {
		raf.write(b, off, len);
	}

	@Override
	public long getPos() throws IOException {
		return raf.getFilePointer();
	}

	@Override
	public long length() throws IOException {
		return raf.length();
	}

	public long toTail() throws IOException {
		long tail = raf.length();
		raf.seek(tail);
		return tail;
	}

	public void close() throws IOException {
		raf.close();
	}

	@Override
	public OutputStream getOutputStream() {
		return this;
	}

	@Override
	public void seek(long offset) throws IOException {
		this.raf.seek(offset);
	}

	@Override
	public int read() throws IOException {
		return raf.read();
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return raf.read(b, off, len);
	}

	@Override
	public int readInt() throws IOException {
		return raf.readInt();
	}

	/**
	 * Delete the bytes from the position of lastUpdateOffset to the end of the
	 * file
	 * 
	 * @param lastUpdateOffset
	 * @throws IOException
	 */
	public void cutOff(long lastUpdateOffset) throws IOException {
		String tempFilePath = filePath + ".backup";
		File tempFile = new File(tempFilePath);
		File normalFile = new File(filePath);
		if (raf != null) {
			raf.close();
		}
		if (normalFile.exists() && normalFile.length() > 0) {
			raf = new RandomAccessFile(normalFile, R_MODE);
			if (tempFile.exists()) {
				tempFile.delete();
			}
			RandomAccessFile tempraf = new RandomAccessFile(tempFile, RW_MODE);
			long offset = 0;
			byte[] tempBytes = new byte[onePassCopySize];

			while (lastUpdateOffset - offset >= onePassCopySize) {
				raf.read(tempBytes);
				tempraf.write(tempBytes);
				offset += onePassCopySize;
			}
			raf.read(tempBytes, 0, (int) (lastUpdateOffset - offset));
			tempraf.write(tempBytes, 0, (int) (lastUpdateOffset - offset));

			tempraf.close();
			raf.close();
		}
		normalFile.delete();
		tempFile.renameTo(normalFile);
		raf = new RandomAccessFile(normalFile, RW_MODE);
	}
}
