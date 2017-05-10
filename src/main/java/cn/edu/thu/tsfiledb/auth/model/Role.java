package cn.edu.thu.tsfiledb.auth.model;

/**
 * @author liukun
 *
 */
public class Role {
	
	private int id;
	private String roleName;
	/**
	 * @param id
	 * @param roleName
	 */
	public Role(int id, String roleName) {
		this.id = id;
		this.roleName = roleName;
	}
	
	public Role(String roleName) {
		this.roleName = roleName;
	}
	
	public Role(){
		
	}

	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * @return the roleName
	 */
	public String getRoleName() {
		return roleName;
	}

	/**
	 * @param roleName the roleName to set
	 */
	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}
	

}
