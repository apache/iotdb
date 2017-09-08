package cn.edu.tsinghua.iotdb.auth.model;

/**
 * @author liukun
 *
 */
public class RolePermission {

	private int id;
	private int roleId;
	private String nodeName;
	private int permissionId;

	/**
	 * @param id
	 * @param roleId
	 * @param permissionId
	 */
	public RolePermission(int id, int roleId, String nodeName, int permissionId) {
		this.id = id;
		this.roleId = roleId;
		this.nodeName = nodeName;
		this.permissionId = permissionId;
	}

	public RolePermission(int roleId, String nodeName, int permissionId) {
		this.roleId = roleId;
		this.nodeName = nodeName;
		this.permissionId = permissionId;
	}

	public RolePermission() {

	}

	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * @return the nodeName
	 */
	public String getNodeName() {
		return nodeName;
	}

	/**
	 * @param nodeName
	 *            the nodeName to set
	 */
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	/**
	 * @return the roleId
	 */
	public int getRoleId() {
		return roleId;
	}

	/**
	 * @param roleId
	 *            the roleId to set
	 */
	public void setRoleId(int roleId) {
		this.roleId = roleId;
	}

	/**
	 * @return the permissionId
	 */
	public int getPermissionId() {
		return permissionId;
	}

	/**
	 * @param permissionId
	 *            the permissionId to set
	 */
	public void setPermissionId(int permissionId) {
		this.permissionId = permissionId;
	}

}
