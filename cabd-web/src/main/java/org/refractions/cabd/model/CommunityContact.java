package org.refractions.cabd.model;

import java.util.UUID;

public class CommunityContact {

	private UUID id;
	private String username;
	
	private String oauthId;
	
	public CommunityContact(UUID id, String username, String oauthId) {
		this.id = id;
		this.username = username;
		this.oauthId = oauthId;
	}
	
	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getOauthId() {
		return this.oauthId;
	}
	
	public void setOauthId(String oauthId) {
		this.oauthId = oauthId;
	}
	
}
