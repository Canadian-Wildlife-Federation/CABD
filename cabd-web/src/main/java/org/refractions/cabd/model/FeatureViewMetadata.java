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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.controllers.AttributeSet;

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
	private static final String ALL_FEATURES_VIEW_EN = "cabd.all_features_view_en";
	private static final String ALL_FEATURES_VIEW_FR = "cabd.all_features_view_fr";
	public static final String ALL_FEATURES_VIEW_ROOT = "cabd.all_features_view";
	
	public static String getAllFeaturesView() {
		if (CabdApplication.isFrench()) return ALL_FEATURES_VIEW_FR;
		return ALL_FEATURES_VIEW_EN;
	}
	/**
	 * attribute key for link to feature 
	 */
	public static final String URL_ATTRIBUTE = "url";
	
	private String featureView;
	private Collection<FeatureViewMetadataField> fields;
	
	public FeatureViewMetadata(String featureView, Collection<FeatureViewMetadataField> fields) {
		this.featureView = featureView;
		this.fields = fields;
	}
	
	public String getFeatureView() {
		if (CabdApplication.isFrench()) return this.featureView + "_fr";
		return this.featureView + "_en";
	}
	
	/**
	 * returns all fields
	 * @return
	 */
	public Collection<FeatureViewMetadataField> getFields(){
		return fields;
	}
	/**
	 * Returns the fields associated with a given attribute set plus the geometry field. If the
	 * set is null it returns all fields 
	 * 
	 * @param set
	 * @return
	 */
	public Collection<FeatureViewMetadataField> getFields(AttributeSet set){
		return getFieldsInternal(set, true);

	}
	
	/**
	 * Returns the fields associated with the given attribute set only - does
	 * not include the geometry field.  If set is null returns all fields
	 * 
	 * @param set
	 * @return
	 */
	public Collection<FeatureViewMetadataField> getFieldsOnly(AttributeSet set){
		return getFieldsInternal(set, false);
	}
	
	private Collection<FeatureViewMetadataField> getFieldsInternal(AttributeSet set, boolean includeGeom){
		if (set == null) return this.fields;
		
		List<FeatureViewMetadataField> filtered = new ArrayList<>();
		for (FeatureViewMetadataField field : this.fields) {
			if (field.getOrder(set) != null) {
				filtered.add(field);
			}else if (field.isGeometry() && includeGeom) {
				filtered.add(field);
			}
		}
		return filtered;
	}
	
}
