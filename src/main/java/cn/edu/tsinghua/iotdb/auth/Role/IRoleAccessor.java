package cn.edu.tsinghua.iotdb.auth.Role;

import cn.edu.tsinghua.iotdb.auth.entity.Role;

import java.io.IOException;
import java.util.List;

/**
 * This interface manages the serialization/deserialization of the role objects.
 */
public interface IRoleAccessor {
    /**
     * Deserialize a role from lower storage.
     * @param rolename The name of the role to be deserialized.
     * @return The role object or null if no such role.
     * @throws IOException
     */
    Role loadRole(String rolename) throws IOException;

    /**
     * Serialize the role object to lower storage.
     * @param role The role object that is to be saved.
     * @throws IOException
     */
    void saveRole(Role role) throws IOException;

    /**
     * Delete a role's in lower storage.
     * @param rolename The name of the role to be deleted.
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
