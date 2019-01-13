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
package org.apache.iotdb.db.postback.sender;

import java.util.Set;

/**
 * @author lta
 */
public interface FileSender {
    /**
     * Connect to server
     */
    void connectToReceiver(String serverIp, int serverPort);

    /**
     * Transfer UUID to receiver
     */
    boolean transferUUID(String uuidPath);

    /**
     * Make file snapshots before sending files
     */
    Set<String> makeFileSnapshot(Set<String> sendingFileList);

    /**
     * Send schema file to receiver.
     */
    void sendSchema(String schemaPath);

    /**
     * For each file in fileList, send it to receiver side
     * 
     * @param fileSnapshotList:snapshot
     *            file list to send
     */
    void startSending(Set<String> fileSnapshotList);

    /**
     * Close socket after send files
     */
    boolean afterSending();

    /**
     * Execute a postback task.
     */
    void postback();

}
