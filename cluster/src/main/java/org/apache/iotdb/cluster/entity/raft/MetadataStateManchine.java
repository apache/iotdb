/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.cluster.entity.raft;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;

public class MetadataStateManchine extends StateMachineAdapter {

  /** All Storage Groups in Cluster **/
  private List<String> storageGroupList;

  /** manager of user profile **/
  private IAuthorizer authorizer = LocalFileAuthorizer.getInstance();

  public MetadataStateManchine() throws AuthException {
    storageGroupList = new ArrayList<>();
    updateStorageGroupList();
  }

  /**
   * update @code{storageGroupList} from IoTDB instance
   */
  private void updateStorageGroupList() {

  }

  // Update StrageGroup List and userProfileMap based on Task read from raft log
  @Override
  public void onApply(Iterator iterator) {

  }

  public boolean isStorageGroupLegal(String sg) {
    return storageGroupList.contains(sg);
  }

  public boolean isUerProfileLegal(String username, String password) throws AuthException {
    return authorizer.login(username, password);
  }

  public void addStorageGroup(String sg) {
    storageGroupList.add(sg);
  }

  public void deleteStorageGroup(String sg) {
    storageGroupList.remove(sg);
  }

  public void addUser(String username, String password) throws AuthException {
    authorizer.createUser(username, password);
  }

  public void deleteUSer(String username, String password) throws AuthException {
    if (isUerProfileLegal(username, password)) {
      authorizer.deleteUser(username);
    }
  }

  public void updateUser(String username, String oldPassword, String newPassword)
      throws AuthException {
    if (isUerProfileLegal(username, oldPassword)) {
      authorizer.updateUserPassword(username, newPassword);
    }
  }
}
