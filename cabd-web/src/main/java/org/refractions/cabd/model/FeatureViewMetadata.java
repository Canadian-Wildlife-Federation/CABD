/*
 * Copyright 2021 Canadian Wildlife Federation
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
	public static final String ALL_FEATURES_VIEW = "cabd.all_features_view";
	
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
