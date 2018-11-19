package cn.edu.tsinghua.iotdb.auth.user;

import cn.edu.tsinghua.iotdb.auth.entity.User;

import java.io.IOException;
import java.util.List;

/**
 * This interface manages the serialization/deserialization of the user objects.
 */
public interface IUserAccessor {

    /**
     * Deserialize a user from lower storage.
     * @param username The name of the user to be deserialized.
     * @return The user object or null if no such user.
     * @throws IOException
     */
    User loadUser(String username) throws IOException;

    /**
     * Serialize the user object to lower storage.
     * @param user The user object that is to be saved.
     * @throws IOException
     */
    void saveUser(User user) throws IOException;

    /**
     * Delete a user's from lower storage.
     * @param username The name of the user to be deleted.
     * @return True if the user is successfully deleted, false if the user does not exists.
     * @throws IOException
     */
    boolean deleteUser(String username) throws IOException;

    /**
     *
     * @return A list that contains names of all users.
     */
    List<String> listAllUsers();

    /**
     * Re-initialize this object.
     */
    void reset();
}
