package org.fathat.model;
/**
 * @author wyhong
 * @date 2018-4-28
 */
public class Enterprise {

	private int id;
	private String name;
	private String address;
	private String description;
	
	public Enterprise(){}
	
	public Enterprise(int id, String name, String address, String description) {
		this.id = id;
		this.name = name;
		this.address = address;
		this.description = description;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	@Override
	public String toString() {
		return "Enterprise [id=" + id + ", name=" + name + ", address="
				+ address + ", description=" + description + "]";
	}
	
	
}
