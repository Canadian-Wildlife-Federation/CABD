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
	private String name;
	private String attributeSourceTable;
	
	private String dataurl;
	private String metadataurl;
	
	private FeatureViewMetadata metadata;
	
	public FeatureType(String type, String dataView, String name, String attributeSourceTable) {
		this.type = type;
		this.dataView = dataView;	
		this.name = name;
		this.attributeSourceTable = attributeSourceTable;
	}

	public String getAttributeSourceTable() {
		return this.attributeSourceTable;
	}
	
	public String getType() {
		return type;
	}

	public String getName() {
		return this.name;
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
