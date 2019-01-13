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
package org.apache.iotdb.db.auth.Role;

import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.Role;

import java.io.IOException;
import java.util.List;

/**
 * This interface manages the serialization/deserialization of the role objects.
 */
public interface IRoleAccessor {
    /**
     * Deserialize a role from lower storage.
     * 
     * @param rolename
     *            The name of the role to be deserialized.
     * @return The role object or null if no such role.
     * @throws IOException
     */
    Role loadRole(String rolename) throws IOException;

    /**
     * Serialize the role object to lower storage.
     * 
     * @param role
     *            The role object that is to be saved.
     * @throws IOException
     */
    void saveRole(Role role) throws IOException;

    /**
     * Delete a role's in lower storage.
     * 
     * @param rolename
     *            The name of the role to be deleted.
     * @return True if the role is successfully deleted, false if the role does not exists.
     * @throws IOException
     */
    boolean deleteRole(String rolename) throws IOException;

    /**
     *
     * @return A list contains all names of the roles.
     */
    List<String> listAllRoles();

    /**
     * Re-initialize this object.
     */
    void reset();
}
