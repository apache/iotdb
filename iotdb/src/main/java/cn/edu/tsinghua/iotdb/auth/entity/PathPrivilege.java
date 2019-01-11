package cn.edu.tsinghua.iotdb.auth.entity;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class represents a privilege on a specific seriesPath. If the privilege is seriesPath-free, the seriesPath will be null.
 */
public class PathPrivilege {
    public Set<Integer> privileges;
    public String path;

    /**
     * This field records how many times this privilege is referenced during a life cycle (from being loaded to being discarded).
     * When serialized to a file, this determines the order of serialization. The higher this values is, the sooner this privilege will
     * be serialized.
     * As a result, the hot privileges will be in the first place so that the hit time will decrease when being queried.
     */
    public AtomicInteger referenceCnt = new AtomicInteger(0);

    public PathPrivilege(String path) {
        this.path = path;
        this.privileges = new HashSet<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PathPrivilege that = (PathPrivilege) o;
        return Objects.equals(privileges, that.privileges) &&
                Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {

        return Objects.hash(privileges, path);
    }

    /**
     * Sort PathPrivilege by referenceCnt in descent order.
     */
    public static Comparator<PathPrivilege> referenceDescentSorter = (o1, o2) -> -Integer.compare(o1.referenceCnt.get(), o2.referenceCnt.get());

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(path);
        builder.append(" :");
        for(Integer privilegeId : privileges) {
            builder.append(" ").append(PrivilegeType.values()[privilegeId]);
        }
        builder.append("\n");
        return builder.toString();
    }
}
