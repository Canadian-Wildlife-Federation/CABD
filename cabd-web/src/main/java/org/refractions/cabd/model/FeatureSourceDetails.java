package org.refractions.cabd.model;

import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;

public class FeatureSourceDetails {

	private String featureName;
	private UUID featureId;
	private List<DataSource> spatialDataSources;
	private List<DataSource> nonSpatialDataSources;
	private List<Pair<String,String>> attributeDataSources;
	
	private boolean includeAllDsDetails = false;
	
	public FeatureSourceDetails(UUID id, boolean includeAllDsDetails) {
		this.featureId = id;
		this.includeAllDsDetails = includeAllDsDetails;
	}
	
	public boolean getIncludeAllDatasourceDetails() {
		return this.includeAllDsDetails;
	}
	public String getFeatureName() {
		return this.featureName;
	}
	
	public UUID getFeatureId() {
		return this.featureId;
	}
	
	public List<DataSource> getSpatialDataSources( ) {
		return this.spatialDataSources;
	}
	
	public List<DataSource> getNonSpatialDataSources( ) {
		return this.nonSpatialDataSources ;
	}
	public List<Pair<String,String>> getAttributeDataSources() {
		return this.attributeDataSources ;
	}
	
	public void setFeatureName(String featureName) {
		this.featureName = featureName;
	}

	public void setSpatialDataSources( List<DataSource> spatialDataSources) {
		this.spatialDataSources = spatialDataSources;
	}
	
	public void setNonSpatialDataSources( List<DataSource> nonSpatialDataSources) {
		this.nonSpatialDataSources = nonSpatialDataSources;
	}
	public void setAttributeDataSources(List<Pair<String,String>> attributeDataSources) {
		this.attributeDataSources = attributeDataSources;
	}
}
