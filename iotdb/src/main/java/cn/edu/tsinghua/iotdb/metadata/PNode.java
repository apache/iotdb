package cn.edu.tsinghua.iotdb.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;

/**
 * PNode is the shorthand for "Property Node", which make up The {@code PTree}  
 * @author zhangjinrui
 *
 */
public class PNode implements Serializable{
	
	private static final long serialVersionUID = -7166236304286006338L;
	private String name;
	private PNode parent;
	private HashMap<String, PNode> children;
	private boolean isLeaf;
	
	/**
	 * This HashMap contains all the {@code MNode} this {@code PNode} is responsible for
	 */
	private LinkedHashMap<String, Integer> linkedMTreePathMap;

	public PNode(String name, PNode parent, boolean isLeaf) {
		this.name = name;
		this.parent = parent;
		this.isLeaf = isLeaf;
		if (!isLeaf) {
			setChildren(new HashMap<>());
		} else {
			linkedMTreePathMap = new LinkedHashMap<>();
		}
	}
	
	public void linkMPath(String mTreePath) throws PathErrorException {
		if(!isLeaf){
			throw new PathErrorException("Current PNode is NOT leaf node");
		}
		if(!linkedMTreePathMap.containsKey(mTreePath)){
			linkedMTreePathMap.put(mTreePath, 1);
		}
	}
	
	public void unlinkMPath(String mTreePath) throws PathErrorException{
		if(!isLeaf){
			throw new PathErrorException("Current PNode is NOT leaf node");
		}
		if(!linkedMTreePathMap.containsKey(mTreePath)){
			return;
		}
		linkedMTreePathMap.remove(mTreePath);
	}
	
	public boolean hasChild(String key) {
		return getChildren().containsKey(key);
	}

	public PNode getChild(String key) {
		return getChildren().get(key);
	}

	public void addChild(String name, PNode node) {
		this.getChildren().put(name, node);
	}
	
	public void deleteChild(String name){
		this.getChildren().remove(name);
	}

	public boolean isLeaf() {
		return isLeaf;
	}

	public void setLeaf(boolean isLeaf) {
		this.isLeaf = isLeaf;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public PNode getParent() {
		return parent;
	}

	public void setParent(PNode parent) {
		this.parent = parent;
	}

	public HashMap<String, PNode> getChildren() {
		return children;
	}

	public void setChildren(HashMap<String, PNode> children) {
		this.children = children;
	}

	public HashMap<String, Integer> getLinkedMTreePathMap() {
		return linkedMTreePathMap;
	}

	public void setLinkedMTreePathMap(LinkedHashMap<String, Integer> linkedMTreePathMap) {
		this.linkedMTreePathMap = linkedMTreePathMap;
	}
}
