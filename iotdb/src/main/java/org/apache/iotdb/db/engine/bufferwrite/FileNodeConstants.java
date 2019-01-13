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
package org.apache.iotdb.db.engine.bufferwrite;

/**
 * Constants for using in bufferwrite, overflow and filenode
 * 
 * @author liukun
 *
 */
public class FileNodeConstants {

    public static final String FILE_NODE_OPERATOR_TYPE = "OPERATOR_TYPE";
    public static final String TIMESTAMP_KEY = "TIMESTAMP";
    public static final String FILE_NODE = "FILE_NODE";
    public static final String CLOSE_ACTION = "CLOSE_ACTION";

    public static final String OVERFLOW_FLUSH_ACTION = "OVERFLOW_FLUSH_ACTION";
    public static final String BUFFERWRITE_FLUSH_ACTION = "BUFFERWRITE_FLUSH_ACTION";
    public static final String BUFFERWRITE_CLOSE_ACTION = "BUFFERWRITE_CLOSE_ACTION";
    public static final String FILENODE_PROCESSOR_FLUSH_ACTION = "FILENODE_PROCESSOR_FLUSH_ACTION";

    public static final String MREGE_EXTENSION = "merge";
    public static final String ERR_EXTENSION = "err";
    public static final String PATH_SEPARATOR = ".";
    public static final String BUFFERWRITE_FILE_SEPARATOR = "-";

}
