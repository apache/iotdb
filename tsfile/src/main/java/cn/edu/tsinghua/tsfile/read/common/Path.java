package cn.edu.tsinghua.tsfile.read.common;

import cn.edu.tsinghua.tsfile.common.constant.SystemConstant;
import cn.edu.tsinghua.tsfile.utils.StringContainer;

/**
 * This class define an Object named Path to represent a series in IoTDB.
 * AndExpression in batch read, this definition is also used in query processing.
 * Note that, Path is unmodified after a new object has been created.
 *
 * @author Kangrong
 */
public class Path {
    private String measurement = null;
    private String device = null;
    private String fullPath;

    public Path(StringContainer pathSc) {
        assert pathSc != null;
        String[] splits = pathSc.toString().split(SystemConstant.PATH_SEPARATER_NO_REGEX);
        init(splits);
    }

    public Path(String pathSc) {
        assert pathSc != null;
        String[] splits = pathSc.split(SystemConstant.PATH_SEPARATER_NO_REGEX);
        init(splits);
    }

    public Path(String[] pathSc) {
        assert pathSc != null;
        String[] splits =
                new StringContainer(pathSc, SystemConstant.PATH_SEPARATOR).toString().split(
                        SystemConstant.PATH_SEPARATER_NO_REGEX);
        init(splits);
    }

    public Path(String device, String measurement) {
        assert device != null && measurement != null;
        String[] splits = (device + SystemConstant.PATH_SEPARATOR + measurement).split(SystemConstant.PATH_SEPARATER_NO_REGEX);
        init(splits);
    }

    private void init(String[] splitedPathArray) {
        StringContainer sc = new StringContainer(splitedPathArray, SystemConstant.PATH_SEPARATOR);
        if (sc.size() <= 1) {
            device = "";
            fullPath = measurement = sc.toString();
        } else {
            device = sc.getSubStringContainer(0, -2).toString();
            measurement = sc.getSubString(-1);
            fullPath = sc.toString();
        }
    }


    public String getFullPath() {
        return fullPath;
    }

    public String getDevice() {
        return device;
    }

    public String getMeasurement() {
        return measurement;
    }

    @Override
    public int hashCode() {
        return fullPath.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj != null && obj instanceof Path && this.fullPath.equals(((Path) obj).fullPath);
    }

    public boolean equals(String obj) {
        return obj != null && this.fullPath.equals(obj);
    }

    @Override
    public String toString() {
        return fullPath;
    }


    @Override
    public Path clone() {
        return new Path(fullPath);
    }

    /**
     * if prefix is null, return false, else judge whether this.fullPath starts with prefix
     *
     * @param prefix the prefix string to be tested.
     * @return True if fullPath starts with prefix
     */
    public boolean startWith(String prefix) {
        return prefix != null && fullPath.startsWith(prefix);
    }

    /**
     * if prefix is null, return false, else judge whether this.fullPath starts with prefix.fullPath
     *
     * @param prefix the prefix path to be tested.
     * @return True if fullPath starts with prefix.fullPath
     */
    public boolean startWith(Path prefix) {
        return startWith(prefix.fullPath);
    }

    public static Path mergePath(Path prefix, Path suffix) {
        StringContainer sc = new StringContainer(SystemConstant.PATH_SEPARATOR);
        sc.addTail(prefix);
        sc.addTail(suffix);
        return new Path(sc);
    }

    /**
     * add {@code prefix}  as the prefix of {@code src}.
     *
     * @param src    to be added.
     * @param prefix the newly prefix
     * @return if this path start with prefix
     */
    public static Path addPrefixPath(Path src, String prefix) {
        StringContainer sc = new StringContainer(SystemConstant.PATH_SEPARATOR);
        sc.addTail(prefix);
        sc.addTail(src);
        return new Path(sc);
    }

    /**
     * add {@code prefix}  as the prefix of {@code src}.
     *
     * @param src    to be added.
     * @param prefix the newly prefix
     * @return <code>Path</code>
     */
    public static Path addPrefixPath(Path src, Path prefix) {
        return addPrefixPath(src, prefix.fullPath);
    }

    /**
     * replace prefix of descPrefix with given parameter {@code srcPrefix}.
     * If the level of the path constructed by {@code srcPrefix} is larger than {@code descPrefix}, return {@code
     * srcPrefix} directly.
     *
     * @param srcPrefix  the prefix to replace descPrefix
     * @param descPrefix to be replaced
     * @return If the level of the path constructed by {@code srcPrefix} is larger than {@code descPrefix}, return
     * {@code srcPrefix} directly.
     */
    public static Path replace(String srcPrefix, Path descPrefix) {
        if ("".equals(srcPrefix) || descPrefix.startWith(srcPrefix))
            return descPrefix;
        int prefixSize = srcPrefix.split(SystemConstant.PATH_SEPARATER_NO_REGEX).length;
        String[] descArray = descPrefix.fullPath.split(SystemConstant.PATH_SEPARATER_NO_REGEX);
        if (descArray.length <= prefixSize)
            return new Path(srcPrefix);
        StringContainer sc = new StringContainer(SystemConstant.PATH_SEPARATOR);
        sc.addTail(srcPrefix);
        for (int i = prefixSize; i < descArray.length; i++) {
            sc.addTail(descArray[i]);
        }
        return new Path(sc);
    }

    /**
     * replace prefix of {@code descPrefix} with given parameter {@code srcPrefix}.
     * If the level of {@code srcPrefix} is larger than {@code descPrefix}, return {@code srcPrefix} directly.
     *
     * @param srcPrefix  the prefix to replace descPrefix
     * @param descPrefix to be replaced
     * @return If the level of {@code srcPrefix} is larger than {@code descPrefix}, return {@code srcPrefix} directly.
     */
    public static Path replace(Path srcPrefix, Path descPrefix) {
        return replace(srcPrefix.fullPath, descPrefix);
    }
}
