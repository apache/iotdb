package cn.edu.thu.tsfiledb.auth.model;

/**
 * @author liukun
 *
 */
public class UserPermission {

	private int id;
	private int userId;
	private String nodeName;
	// the permissionId should be from Permission class
	private int permissionId;

	/**
	 * @param id
	 * @param userId
	 * @param nodeName
	 * @param permissionId
	 */
	public UserPermission(int id, int userId, String nodeName, int permissionId) {
		this.id = id;
		this.userId = userId;
		this.nodeName = nodeName;
		this.permissionId = permissionId;
	}

	public UserPermission() {

	}

	public UserPermission(int userId, String nodeName, int permissionId) {
		this.userId = userId;
		this.nodeName = nodeName;
		this.permissionId = permissionId;
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
	 * @return the userId
	 */
	public int getUserId() {
		return userId;
	}

	/**
	 * @param userId
	 *            the userId to set
	 */
	public void setUserId(int userId) {
		this.userId = userId;
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
