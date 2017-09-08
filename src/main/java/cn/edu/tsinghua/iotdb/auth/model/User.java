package cn.edu.tsinghua.iotdb.auth.model;

/**
 * @author liukun
 *
 */
public class User {

	private int id;
	private String userName;
	private String passWord;
	private boolean locked;// true - t false - f
	private String validTime;

	/**
	 * @param id
	 * @param userName
	 * @param passWord
	 * @param isLock
	 * @param validTime
	 */
	public User(int id, String userName, String passWord, boolean isLock, String validTime) {
		this.id = id;
		this.userName = userName;
		this.passWord = passWord;
		this.locked = isLock;
		this.validTime = validTime;
	}

	public User() {

	}

	public User(String userName, String passWord) {
		this.userName = userName;
		this.passWord = passWord;
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
	 * @return the userName
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * @param userName
	 *            the userName to set
	 */
	public void setUserName(String userName) {
		this.userName = userName;
	}

	/**
	 * @return the passWord
	 */
	public String getPassWord() {
		return passWord;
	}

	/**
	 * @param passWord
	 *            the passWord to set
	 */
	public void setPassWord(String passWord) {
		this.passWord = passWord;
	}

	/**
	 * @return the isLock
	 */
	public boolean isLock() {
		return locked;
	}

	/**
	 * @param isLock
	 *            the isLock to set
	 */
	public void setLock(boolean isLock) {
		this.locked = isLock;
	}

	/**
	 * @return the validTime
	 */
	public String getValidTime() {
		return validTime;
	}

	/**
	 * @param validTime
	 *            the validTime to set
	 */
	public void setValidTime(String validTime) {
		this.validTime = validTime;
	}

}
