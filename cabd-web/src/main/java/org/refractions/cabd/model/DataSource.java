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
	private String name;
	private Date versionDate;
	private String version;
	private String featureId;
	private String type;
	
	public DataSource(UUID id, String name, String type, Date versionDate, String version, String featureId) {
		this.id = id;
		this.name = name;
		this.versionDate = versionDate;
		this.version = version;
		this.featureId = featureId;
		this.type = type;
	}
	
	public UUID getId() {
		return id;
	}
	public String getName() {
		return name;
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
}
