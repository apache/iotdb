/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.io;

import org.apache.iotdb.tsfile.common.utils.ITsRandomAccessFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;


/**
 * This class is used to wrap the {@link}FSDataOutputStream and implement the
 * interface {@link}TSRandomAccessFileWriter
 *
 */
public class HDFSOutputStream implements ITsRandomAccessFileWriter {

	private FSDataOutputStream fsDataOutputStream;

	public HDFSOutputStream(String filePath, boolean overwriter) throws IOException {
		
		this(filePath, new Configuration(), overwriter);
	}

	
	public HDFSOutputStream(String filePath, Configuration configuration, boolean overwriter) throws IOException {
		
		this(new Path(filePath),configuration,overwriter);
	}
	
	public HDFSOutputStream(Path path,Configuration configuration,boolean overwriter) throws IOException{
		
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
