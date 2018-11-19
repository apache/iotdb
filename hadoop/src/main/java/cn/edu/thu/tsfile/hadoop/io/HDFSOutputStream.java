package cn.edu.thu.tsfile.hadoop.io;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;

/**
 * This class is used to wrap the {@link}FSDataOutputStream and implement the
 * interface {@link}TSRandomAccessFileWriter
 *
 * @author liukun
 */
public class HDFSOutputStream implements ITsRandomAccessFileWriter {

	private FSDataOutputStream fsDataOutputStream;

	public HDFSOutputStream(String filePath, boolean overwriter) throws IOException {

		this(filePath, new Configuration(), overwriter);
	}

	public HDFSOutputStream(String filePath, Configuration configuration, boolean overwriter) throws IOException {

		this(new Path(filePath), configuration, overwriter);
	}

	public HDFSOutputStream(Path path, Configuration configuration, boolean overwriter) throws IOException {

		FileSystem fsFileSystem = FileSystem.get(configuration);
		fsDataOutputStream = fsFileSystem.create(path, overwriter);
	}

	@Override
	public OutputStream getOutputStream() {

		return fsDataOutputStream;
	}

	@Override
	public long getPos() throws IOException {

		return fsDataOutputStream.getPos();
	}

	@Override
	public void seek(long offset) throws IOException {
		throw new IOException("Not support");
	}

	@Override
	public void write(int b) throws IOException {

		fsDataOutputStream.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {

		fsDataOutputStream.write(b);
	}

	@Override
	public void close() throws IOException {

		fsDataOutputStream.close();
	}

}
