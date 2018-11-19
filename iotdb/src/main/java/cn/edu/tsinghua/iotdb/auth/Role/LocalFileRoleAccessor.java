package cn.edu.tsinghua.iotdb.auth.Role;

import cn.edu.tsinghua.iotdb.auth.entity.PathPrivilege;
import cn.edu.tsinghua.iotdb.auth.entity.Role;
import cn.edu.tsinghua.iotdb.conf.TsFileDBConstant;
import cn.edu.tsinghua.iotdb.utils.IOUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * This class store each role in a separate sequential file.
 * Role file schema :
 *      Int32 role name length
 *      Utf-8 role name bytes
 *      Int32 path privilege number n
 *          Int32 path[1] length
 *          Utf-8 path[1] bytes
 *          Int32 privilege num k1
 *              Int32 privilege[1][1]
 *              Int32 privilege[1][2]
 *              ...
 *              Int32 privilege[1][k1]
 *          Int32 path[2] length
 *          Utf-8 path[2] bytes
 *          Int32 privilege num k2
 *              Int32 privilege[2][1]
 *              Int32 privilege[2][2]
 *              ...
 *              Int32 privilege[2][k2]
 *          ...
 *          Int32 path[n] length
 *          Utf-8 path[n] bytes
 *          Int32 privilege num kn
 *              Int32 privilege[n][1]
 *              Int32 privilege[n][2]
 *              ...
 *              Int32 privilege[n][kn]
 */
public class LocalFileRoleAccessor implements IRoleAccessor {

    private static final String TEMP_SUFFIX = ".temp";
    private static final String STRING_ENCODING = "utf-8";

    private String roleDirPath;

    /**
     * Reused buffer for primitive types encoding/decoding, which aim to reduce memory fragments.
     * Use ThreadLocal for thread safety.
     */
    private ThreadLocal<ByteBuffer> encodingBufferLocal = new ThreadLocal<>();
    private ThreadLocal<byte[]> strBufferLocal = new ThreadLocal<>();

    public LocalFileRoleAccessor(String roleDirPath) {
        this.roleDirPath = roleDirPath;
    }

    @Override
    public Role loadRole(String rolename) throws IOException {
        File roleProfile = new File(roleDirPath + File.separator + rolename + TsFileDBConstant.PROFILE_SUFFIX);
        if(!roleProfile.exists() || !roleProfile.isFile()) {
            // System may crush before a newer file is written, so search for back-up file.
            File backProfile = new File(roleDirPath + File.separator + rolename + TsFileDBConstant.PROFILE_SUFFIX + TEMP_SUFFIX);
            if(backProfile.exists() && backProfile.isFile())
                roleProfile = backProfile;
            else
                return null;
        }
        FileInputStream inputStream = new FileInputStream(roleProfile);
        try (DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(inputStream))) {
            Role role = new Role();
            role.name = IOUtils.readString(dataInputStream, STRING_ENCODING, strBufferLocal);

            int privilegeNum = dataInputStream.readInt();
            List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
            for (int i = 0; i < privilegeNum; i++) {
                pathPrivilegeList.add(IOUtils.readPathPrivilege(dataInputStream, STRING_ENCODING, strBufferLocal));
            }
            role.privilegeList = pathPrivilegeList;

            return role;
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public void saveRole(Role role) throws IOException {
        File roleProfile = new File(roleDirPath + File.separator + role.name + TsFileDBConstant.PROFILE_SUFFIX + TEMP_SUFFIX);
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(roleProfile));
        try {
            IOUtils.writeString(outputStream, role.name, STRING_ENCODING, encodingBufferLocal);

            role.privilegeList.sort(PathPrivilege.referenceDescentSorter);
            int privilegeNum = role.privilegeList.size();
            IOUtils.writeInt(outputStream, privilegeNum, encodingBufferLocal);
            for(int i = 0; i < privilegeNum; i++) {
                PathPrivilege pathPrivilege = role.privilegeList.get(i);
                IOUtils.writePathPrivilege(outputStream, pathPrivilege, STRING_ENCODING, encodingBufferLocal);
            }

        } catch (Exception e) {
            throw new IOException(e.getMessage());
        } finally {
            outputStream.flush();
            outputStream.close();
        }
        
        File oldFile = new File(roleDirPath + File.separator + role.name + TsFileDBConstant.PROFILE_SUFFIX);
        IOUtils.replaceFile(roleProfile, oldFile);
    }

    @Override
    public boolean deleteRole(String rolename) throws IOException {
        File roleProfile = new File(roleDirPath + File.separator + rolename + TsFileDBConstant.PROFILE_SUFFIX);
        File backFile = new File(roleDirPath + File.separator + rolename + TsFileDBConstant.PROFILE_SUFFIX + TEMP_SUFFIX);
        if(!roleProfile.exists() && !backFile.exists())
            return false;
        if((roleProfile.exists() && !roleProfile.delete()) || (backFile.exists() && !backFile.delete())) {
            throw new IOException(String.format("Cannot delete role file of %s", rolename));
        }
        return true;
    }

    @Override
    public List<String> listAllRoles() {
        File roleDir = new File(roleDirPath);
        String[] names = roleDir.list((dir, name) -> name.endsWith(TsFileDBConstant.PROFILE_SUFFIX) || name.endsWith(TEMP_SUFFIX));
        List<String> retList = new ArrayList<>();
        if(names != null) {
            // in very rare situations, normal file and backup file may exist at the same time
            // so a set is used to deduplicate
            Set<String> set = new HashSet<>();
            for(String fileName : names) {
                set.add(fileName.replace(TsFileDBConstant.PROFILE_SUFFIX, "").replace(TEMP_SUFFIX, ""));
            }
            retList.addAll(set);
        }
        return retList;
    }

    @Override
    public void reset() {
        new File(roleDirPath).mkdirs();
    }
}
