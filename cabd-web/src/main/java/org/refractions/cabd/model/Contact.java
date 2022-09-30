package org.refractions.cabd.model;

import java.util.UUID;

public class Contact {

	private UUID id;
	private String email;
	private String name;
	private String organization;
	private UUID dataSource;
	
	public Contact(UUID id, String email, String name, String organization, UUID dataSource) {
		this.id = id;
		this.email = email;
		this.name = name;
		this.organization = organization;
		this.dataSource = dataSource;
	}
	
	public Contact(String email, String name, String organization) {
		this(null, email, name, organization, null);
	}
	
	public UUID getId() {
		return this.id;
	}
	
	public String getEmail() {
		return this.email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	
	public String getName() {
		return this.name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public String getOrganization() {
		return this.organization;
	}
	public void setOrganization(String organization) {
		this.organization = organization;
	}
	
	public UUID getDataSource() {
		return this.dataSource;
	}
	
	public void setDataSource(UUID dataSource) {
		this.dataSource = dataSource;
	}
}
