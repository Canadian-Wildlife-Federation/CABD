/*
 * Copyright 2022 Canadian Wildlife Federation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */
package org.refractions.cabd.model;

import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Feature Source Details 
 * 
 * @author Emily
 *
 */
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
