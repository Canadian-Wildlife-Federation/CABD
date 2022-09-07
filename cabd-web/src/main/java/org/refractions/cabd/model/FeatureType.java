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

import org.refractions.cabd.CabdApplication;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Represents a feature type.
 * 
 * @author Emily
 *
 */
public class FeatureType extends NamedItem{

	private String type;
	private String dataView;
	
	private String attributeSourceTable;
	private String featureSourceTable;
	private String defaultNameField;
	
	private String dataurl;
	private String metadataurl;
	
	private FeatureViewMetadata metadata;
	
	public FeatureType(String type, String dataView, String name_en, String name_fr, String attributeSourceTable, 
			String featureSourceTable, String defaultNameField) {
		super(name_en, name_fr);
		this.type = type;
		this.dataView = dataView;
		this.attributeSourceTable = attributeSourceTable;
		this.featureSourceTable = featureSourceTable;
		this.defaultNameField = defaultNameField;
	}

	public String getFeatureSourceTable() {
		return this.featureSourceTable;
	}
	public String getDefaultNameField() {
		return this.defaultNameField;
	}
	
	public String getAttributeSourceTable() {
		return this.attributeSourceTable;
	}
	
	public String getType() {
		return type;
	}

	public String getDataUrl() {
		return this.dataurl;
	}
	
	public String getMetadataUrl() {
		return this.metadataurl;
	}
	
	public void setUrls(String data, String metadata) {
		this.dataurl = data + "/" + getType();
		this.metadataurl = metadata + "/" + getType();
	}
	
	/**
	 * Returns the "core" name of the view containing the metadata
	 * for the feature type. This is the name referenced in the metadata table.
	 *  If you want the actual view with the data
	 * use the getDataView() function
	 * @return
	 */
	@JsonIgnore
	public String getDataViewName() {
		return this.dataView;
	}
	
	/**
	 * Returns the name of the view in the database containing the data
	 * for this feature type AND the request local
	 * @return
	 */
	@JsonIgnore
	public String getDataView() {
		if (CabdApplication.isFrench()) {
			return this.dataView + "_fr";
		}
		return this.dataView + "_en";
	}

	@JsonIgnore
	public FeatureViewMetadata getViewMetadata() {
		return this.metadata;
	}
	
	public void setViewMetadata(FeatureViewMetadata metadata) {
		this.metadata = metadata;
	}
	
}
