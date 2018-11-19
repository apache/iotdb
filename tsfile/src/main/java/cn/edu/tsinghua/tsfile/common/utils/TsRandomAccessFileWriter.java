package cn.edu.tsinghua.tsfile.common.utils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * RandomAccessOutputStream implements the tsfile file writer interface and extends OutputStream. <br>
 * The main difference between RandomAccessOutputStream and general OutputStream
 * is:RandomAccessOutputStream provide method {@code getPos} for random accessing. It also
 * implements {@code getOutputStream} to return an OutputStream supporting tsfile-format
 *
 * @author kangrong
 */
public class TsRandomAccessFileWriter implements ITsRandomAccessFileWriter {
	private static final String DEFAULT_FILE_MODE = "rw";
	private RandomAccessFile out;
	private OutputStream outputStream;

	public TsRandomAccessFileWriter(File file) throws IOException {
		this(file, DEFAULT_FILE_MODE);
	}

	public TsRandomAccessFileWriter(File file, String mode) throws IOException {
		out = new RandomAccessFile(file, mode);
		outputStream=new OutputStream() {
			@Override
			public void write(int b) throws IOException {
				TsRandomAccessFileWriter.this.write(b);
			}
			@Override
			public void write(byte b[], int off, int len) throws IOException {
				out.write(b, off, len);
			}
			@Override
			public void write(byte b[]) throws IOException {
				TsRandomAccessFileWriter.this.write(b);
			}

			@Override
			public void close() throws IOException {
				TsRandomAccessFileWriter.this.close();
			}
		};
	}
	
	@Override
	public void write(int b) throws IOException {
		out.write(b);
	}

	@Override
	public void write(byte b[]) throws IOException {
		out.write(b);
	}

	@Override
	public long getPos() throws IOException {
		return out.length();
	}

	@Override
	public void seek(long offset) throws IOException {
		out.seek(offset);
	}

	@Override
	public void close() throws IOException {
		out.close();
	}

	@Override
	public OutputStream getOutputStream() {
		return outputStream;
	}
}
