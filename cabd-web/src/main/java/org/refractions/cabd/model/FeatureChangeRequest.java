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

import java.util.regex.Pattern;

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
	public String getDataSource() {
		return dataSource;
	}
	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}
	
	public String validate() {
		if (this.getEmail() == null || this.getEmail().isBlank()) return "Email is required";
		//regex validate email
		//https://www.oreilly.com/library/view/regular-expressions-cookbook/9781449327453/ch04s01.html
		//basic email check
		String regex = "\\A[A-Z0-9+_.-]+@[A-Z0-9.-]+\\Z";
		if (!Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(this.getEmail()).find())
			return "Email is invalid. Must be of the form localpart@domain.com";

		if (this.getDescription() == null || this.getDescription().isBlank()) return "Description is required";		
		
		return null;
	}
	
	
}
