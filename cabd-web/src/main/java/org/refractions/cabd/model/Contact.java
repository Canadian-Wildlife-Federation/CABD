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
	
	public UUID getDataSource() {
		return this.dataSource;
	}
	
	public void setDataSource(UUID dataSource) {
		this.dataSource = dataSource;
	}
}
