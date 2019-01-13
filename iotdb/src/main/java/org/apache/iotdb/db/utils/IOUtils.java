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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.auth.entity.PathPrivilege;
import org.apache.iotdb.db.auth.entity.PrivilegeType;

import java.io.*;
import java.nio.ByteBuffer;

public class IOUtils {

    /*
     * In the following methods, you may pass a ThreadLocal buffer to avoid frequently memory allocation. You may also
     * pass a null to use a local buffer.
     */
    public static void writeString(OutputStream outputStream, String str, String encoding,
            ThreadLocal<ByteBuffer> encodingBufferLocal) throws IOException {
        if (str != null) {
            byte[] strBuffer = str.getBytes(encoding);
            writeInt(outputStream, strBuffer.length, encodingBufferLocal);
            outputStream.write(strBuffer);
        } else {
            writeInt(outputStream, 0, encodingBufferLocal);
        }
    }

    public static void writeInt(OutputStream outputStream, int i, ThreadLocal<ByteBuffer> encodingBufferLocal)
            throws IOException {
        ByteBuffer encodingBuffer;
        if (encodingBufferLocal != null) {
            encodingBuffer = encodingBufferLocal.get();
            if (encodingBuffer == null) {
                // set to 8 because this buffer may be applied to other types
                encodingBuffer = ByteBuffer.allocate(8);
                encodingBufferLocal.set(encodingBuffer);
            }
        } else {
            encodingBuffer = ByteBuffer.allocate(4);
        }
        encodingBuffer.clear();
        encodingBuffer.putInt(i);
        outputStream.write(encodingBuffer.array(), 0, Integer.BYTES);
    }

    public static String readString(DataInputStream inputStream, String encoding, ThreadLocal<byte[]> strBufferLocal)
            throws IOException {
        byte[] strBuffer;
        int length = inputStream.readInt();
        if (length > 0) {
            if (strBufferLocal != null) {
                strBuffer = strBufferLocal.get();
                if (strBuffer == null || length > strBuffer.length) {
                    strBuffer = new byte[length];
                    strBufferLocal.set(strBuffer);
                }
            } else {
                strBuffer = new byte[length];
            }

            inputStream.read(strBuffer, 0, length);
            return new String(strBuffer, 0, length, encoding);
        } else
            return null;
    }

    public static PathPrivilege readPathPrivilege(DataInputStream inputStream, String encoding,
            ThreadLocal<byte[]> strBufferLocal) throws IOException {
        String path = IOUtils.readString(inputStream, encoding, strBufferLocal);
        int privilegeNum = inputStream.readInt();
        PathPrivilege pathPrivilege = new PathPrivilege(path);
        for (int i = 0; i < privilegeNum; i++)
            pathPrivilege.privileges.add(inputStream.readInt());
        return pathPrivilege;
    }

    public static void writePathPrivilege(OutputStream outputStream, PathPrivilege pathPrivilege, String encoding,
            ThreadLocal<ByteBuffer> encodingBufferLocal) throws IOException {
        writeString(outputStream, pathPrivilege.path, encoding, encodingBufferLocal);
        writeInt(outputStream, pathPrivilege.privileges.size(), encodingBufferLocal);
        for (Integer i : pathPrivilege.privileges) {
            writeInt(outputStream, i, encodingBufferLocal);
        }
    }

    /**
     * Replace newFile with oldFile. If the file system does not support atomic file replacement then delete the old
     * file first.
     * 
     * @param newFile
     * @param oldFile
     */
    public static void replaceFile(File newFile, File oldFile) throws IOException {
        if (!newFile.renameTo(oldFile)) {
            // some OSs need to delete the old file before renaming to it
            oldFile.delete();
            if (!newFile.renameTo(oldFile))
                throw new IOException(
                        String.format("Cannot replace old user file with new one : %s", newFile.getPath()));
        }
    }
}
