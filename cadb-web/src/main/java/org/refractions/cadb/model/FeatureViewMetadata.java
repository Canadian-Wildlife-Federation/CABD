package org.refractions.cadb.model;

import java.util.Collection;

/**
 * Represents the metadata about a view containing
 * feature details. 
 * 
 * @author Emily
 *
 */
public class FeatureViewMetadata {

	/**
	 * The database view that lists all features
	 */
	public static final String ALL_FEATURES_VIEW = "cadb.all_features_view";
	
	private String featureView;
	private Collection<FeatureViewMetadataField> fields;
	
	public FeatureViewMetadata(String featureView, Collection<FeatureViewMetadataField> fields) {
		this.featureView = featureView;
		this.fields = fields;
	}
	
	public String getFeatureView() {
		return this.featureView;
	}
	
	public Collection<FeatureViewMetadataField> getFields(){
		return this.fields;
	}
	
	
	
}
