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

/**
 * Feature change request object.
 * 
 * @author Emily
 *
 */
public class FeatureChangeRequest {

	private String email;
	private String name;
	private String organization;
	private String description;
	private String dataSource;
	private Boolean mailinglist;
	
	public FeatureChangeRequest() {
		
	}
	
	public String getEmail() {
		return email;
	}
	
	/**
	 * emails are always converted to lower case
	 * @param email
	 */
	public void setEmail(String email) {
		this.email = email.toLowerCase();
	}
	
	public Boolean getMailinglist() {
		return this.mailinglist;
	}
	
	public void setMailinglist(Boolean mailinglist) {
		this.mailinglist = mailinglist;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getOrganization() {
		return organization;
	}
	public void setOrganization(String organization) {
		this.organization = organization;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getDatasource() {
		return dataSource;
	}
	public void setDatasource(String dataSource) {
		this.dataSource = dataSource;
	}
	
	/**
	 * Validates the change request. 
	 * 
	 * Validation requires a valid email address and an update description
	 * @return
	 */
	public String validate() {
		String email = Contact.validateEmail(getEmail());
		if (email != null) return email;

		if (this.getDescription() == null || this.getDescription().isBlank()) return "Description is required";				
		return null;
	}
	
	
}
