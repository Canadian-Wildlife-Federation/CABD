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

/**
 * Feature data source details.
 * 
 * @author Emily
 *
 */
public class DataSource {

	private UUID id;
	private String shortName;
	private Date versionDate;
	private String version;
	private String featureId;
	private String type;
	private String source;
	
	private String organization;
	private String category;
	private String geoCoverage;
	private String license;
	private String fullname;
	
	private String sourceIdField;
	
	
	public DataSource(UUID id, String shortName, String type, Date versionDate, String version, String featureId) {
		this.id = id;
		this.shortName = shortName;
		this.versionDate = versionDate;
		this.version = version;
		this.featureId = featureId;
		this.type = type;
	}
	
	public DataSource(UUID id, String shortName, String type, Date versionDate, String version,
			String organization, String category, String geoCoverage, String licence, String fullname, 
			String source, String sourceIdField) {
		this(id, shortName, type, versionDate, version, null);
		
		this.organization = organization;
		this.category = category;
		this.geoCoverage = geoCoverage;
		this.license = licence;
		this.fullname = fullname;
		this.source = source;
		this.sourceIdField = sourceIdField;
	}
	
	public UUID getId() {
		return id;
	}
	public String getFullName() {
		return this.fullname;
	}
	/**
	 * 
	 * @return the short name of the data source
	 */
	public String getName() {
		return shortName;
	}
	public Date getVersionDate() {
		return versionDate;
	}
	public String getVersion() {
		return version;
	}	
	public String getFeatureId() {
		return featureId;
	}
	public String getType() {
		return this.type;
	}
	public String getCategory() {
		return this.category;
	}
	public String getOrganizationName() {
		return this.organization;
	}
	public String getGeographicCoverage() {
		return this.geoCoverage;
	}
	public String getLicense() {
		return this.license;
	}
	public String getSource() {
		return this.source;
	}
	public String getSourceFieldId() {
		return this.sourceIdField;
	}
}
