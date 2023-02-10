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

import java.util.UUID;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Contact model object 
 * 
 * @author Emily
 *
 */
public class Contact {

	private UUID id;
	private String email;
	private String name;
	private String organization;
	private UUID dataSource;
	
	public Contact() {
		
	}
	public Contact(UUID id, String email, String name, String organization, UUID dataSource) {
		this.id = id;
		this.email = email;
		this.name = name;
		this.organization = organization;
		this.dataSource = dataSource;
	}
	
	public Contact(String email, String name, String organization) {
		this(null, email, name, organization, null);
	}
	
	public UUID getId() {
		return this.id;
	}
	
	public String getEmail() {
		return this.email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	
	public String getName() {
		return this.name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public String getOrganization() {
		return this.organization;
	}
	public void setOrganization(String organization) {
		this.organization = organization;
	}
	
	@JsonIgnore
	public UUID getDataSource() {
		return this.dataSource;
	}
	
	@JsonIgnore
	public void setDataSource(UUID dataSource) {
		this.dataSource = dataSource;
	}
	
	/**
	 * Performs a required and basic regex expression check for
	 * email and returns error message or null if no error
	 * 
	 * @param email
	 * @return
	 */
	public static final String validateEmail(String email) {
		if (email == null || email.isBlank()) return "Email is required";
		//regex validate email
		//https://www.oreilly.com/library/view/regular-expressions-cookbook/9781449327453/ch04s01.html
		//basic email check
		String regex = "\\A[A-Z0-9+_.-]+@[A-Z0-9.-]+\\Z";
		if (!Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(email).find())
			return "Email is invalid. Must be of the form localpart@domain.com";

		return null;
	}
}
