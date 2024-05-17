package org.refractions.cabd.model;

import java.util.UUID;

import com.google.gson.JsonObject;

public class CommunityFeature {
	
	private CommunityData rawData;
	
	private UUID cabdId;
	
	
	private String featureType;
	
	private String username;
	private CommunityContact contact;
	
	private JsonObject propertiesJson;
	
	private int index;
	
	
	public CommunityFeature(UUID cabdId, String featureType, String username, JsonObject properties) {
		this.cabdId = cabdId;
		this.featureType = featureType;
		this.username = username;
		this.propertiesJson = properties;
	}
	
	public int getIndex() {
		return this.index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	
	public void setCabdId(UUID cabdId) {
		this.cabdId = cabdId;
	}

	public String getFeatureType() {
		return this.featureType;
	}
	
	public void setRaw(CommunityData data) {
		this.rawData = data;
	}
	public CommunityData getRawData() {
		return this.rawData;
	}

	public JsonObject getProperties() {
		return this.propertiesJson;
	}
	
	public String getUsername() {
		return this.username;
	}
	
	
	public UUID getCabdId() {
		return this.cabdId;
	}
	
	public CommunityContact getCommunityContact() {
		return this.contact;
	}
	
	public JsonObject getJson() {
		return this.propertiesJson;
	}
	
	public String getJsonDataAsString() {
		return this.propertiesJson.toString();
	}
	
	public void setCommunityContact(CommunityContact contact) {
		this.contact = contact;
	}
}
