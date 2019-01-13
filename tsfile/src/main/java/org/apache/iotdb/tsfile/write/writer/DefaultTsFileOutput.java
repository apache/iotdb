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
package org.apache.iotdb.tsfile.write.writer;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * a TsFileOutput implementation with FileOutputStream. If the file is not existed, it will be created. Otherwise the
 * file will be written from position 0.
 */
public class DefaultTsFileOutput implements TsFileOutput {

    FileOutputStream outputStream;

    public DefaultTsFileOutput(File file) throws FileNotFoundException {
        this.outputStream = new FileOutputStream(file);
    }

    public DefaultTsFileOutput(FileOutputStream outputStream) throws FileNotFoundException {
        this.outputStream = outputStream;
    }

    @Override
    public void write(byte[] b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void write(ByteBuffer b) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public long getPosition() throws IOException {
        return outputStream.getChannel().position();
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }

    @Override
    public OutputStream wrapAsStream() throws IOException {
        return outputStream;
    }

    @Override
    public void flush() throws IOException {
        this.outputStream.flush();
    }

    @Override
    public void truncate(long position) throws IOException {
        outputStream.getChannel().truncate(position);
    }

}
