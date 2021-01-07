package org.refractions.cadb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Represents a feature type.
 * 
 * @author Emily
 *
 */
public class FeatureType {

	private String type;
	private String dataView;
	
	private String url;
	
	private FeatureViewMetadata metadata;
	
	public FeatureType(String type, String dataView) {
		this.type = type;
		this.dataView = dataView;	
	}

	public String getType() {
		return type;
	}

	public String getUrl() {
		return this.url;
	}
	
	public void setUrl(String root) {
		this.url = root + "/" + getType();
	}
	
	@JsonIgnore
	public String getDataView() {
		return this.dataView;
	}

	@JsonIgnore
	public FeatureViewMetadata getViewMetadata() {
		return this.metadata;
	}
	
	public void setViewMetadata(FeatureViewMetadata metadata) {
		this.metadata = metadata;
	}
	
}
