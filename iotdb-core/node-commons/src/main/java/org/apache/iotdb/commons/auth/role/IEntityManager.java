/*
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
package org.apache.iotdb.commons.auth.role;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;

import java.util.List;

/** We can call user or role as entity of access control, they all can obtain privileges */
public interface IEntityManager extends SnapshotProcessor {

  /**
   * Get an entity object.
   *
   * @param entityName The name of the entity.
   * @return An entity object whose name is entityName or null if such entity does not exist.
   * @throws AuthException if exception is raised while getting the entity.
   */
  Role getEntity(String entityName) throws AuthException;

  /**
   * Get an entity object.
   *
   * @param entityId The id of the entity.
   * @return An entity object whose index is entityId or null if such entity does not exist.
   * @throws AuthException if exception is raised while getting the entity.
   */
  Role getEntity(long entityId) throws AuthException;

  /**
   * Delete an entity.
   *
   * @param entityName the name of the entity.
   * @return boolean, true means we have the entity in entityManager.
   */
  boolean deleteEntity(String entityName);

  /**
   * Grant a privilege to an entity.
   *
   * @param entityName The name of the entity to which the privilege should be added.
   * @param privilegeUnion The privilege will be granted to entity.
   * @throws AuthException If the entity does not exist or the privilege or the path is illegal.
   */
  void grantPrivilegeToEntity(String entityName, PrivilegeUnion privilegeUnion)
      throws AuthException;

  /**
   * Revoke a privilege on path from an entity.
   *
   * @param entityName The name of the entity from which the privilege should be removed.
   * @param privilegeUnion The privilege will be granted to entity.
   * @throws AuthException If the entity does not exist or the privilege or the path is illegal.
   */
  void revokePrivilegeFromEntity(String entityName, PrivilegeUnion privilegeUnion)
      throws AuthException;

  /** Re-initialize this object. */
  void reset() throws AuthException;

  /**
   * List all entities.
   *
   * @return A list that contains names of all entities.
   */
  List<String> listAllEntities();
}
