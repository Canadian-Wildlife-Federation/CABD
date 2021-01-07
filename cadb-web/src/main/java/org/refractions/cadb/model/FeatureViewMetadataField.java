package org.refractions.cadb.model;

/**
 * Contains details about a feature
 * view field.
 * 
 * @author Emily
 *
 */
public class FeatureViewMetadataField {

	private String fieldName;
	private String name;
	private String description;

	private boolean isLink = false;
	
	private boolean isGeometry = false;
	private Integer srid = null;
	
	public FeatureViewMetadataField(String fieldName, String name, String description, boolean isLink) {
		this.fieldName = fieldName;
		this.name = name;
		this.description = description;
		this.isLink = isLink;
	}
	
	public boolean isLink() {
		return this.isLink;
	}
	
	public String getFieldName() {
		return fieldName;
	}
	public String getName() {
		return name;
	}
	public String getDescription() {
		return description;
	}
	
	public boolean isGeometry() {
		return this.isGeometry;
	}
	
	public void setGeometry(boolean geometry, Integer srid) {
		this.isGeometry = geometry;
		this.srid = srid;
	}
	
	public Integer getSRID() {
		return this.srid;
	}
	
}
