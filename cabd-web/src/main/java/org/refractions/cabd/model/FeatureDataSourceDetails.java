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

import java.util.Date;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Details about a data source of a given feature attribute 
 * 
 * @author Emily
 *
 */
public class FeatureDataSourceDetails {

	private UUID cabdId;
	private String attributeFieldName;
	private String attributeName;
	private DataSource ds;
	private String dsFeatureId;

	
	public FeatureDataSourceDetails(UUID uuid) {
		this.cabdId = uuid;
	}
	
	@JsonProperty("cabd_id")
	public UUID getCabdId() {
		return cabdId;
	}

	@JsonProperty("attribute_field_name")
	public String getAttributeFieldName() {
		return attributeFieldName;
	}
	public void setAttributeFieldName(String attributeFieldName) {
		this.attributeFieldName = attributeFieldName;
	}
	
	@JsonProperty("attribute_name")
	public String getAttributeName() {
		return attributeName;
	}
	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}
	
	public void setDataSource(DataSource ds) {
		this.ds = ds;
	}
	
	@JsonIgnore
	public UUID getDsId() {
		if (ds == null) return null;
		return ds.getId();
	}
	
	@JsonProperty("datasource_feature_id")
	public String getDsFeatureId() {
		return dsFeatureId;
	}
	
	public void setDsFeatureId(String dsFeatureId) {
		this.dsFeatureId = dsFeatureId;
	}
	@JsonProperty("datasource_name")
	public String getDsName() {
		if (ds == null) return null;
		return ds.getName();
	}
	
	@JsonProperty("datasource_date")
	public Date getDsDate() {
		if (ds == null) return null;
		return ds.getVersionDate();
	}

	@JsonProperty("datasource_version")
	public String getDsVersion() {
		if (ds == null) return null;
		return ds.getVersion();
	}
	
}
