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

/**
 * Valid list item for code/list attributes
 * 
 * @author Emily
 *
 */
public class FeatureTypeListValue {

	private Object value;
	private String name;
	private String description;
	
	public FeatureTypeListValue(Object value, String name, String description) {
		this.value = value;
		this.name = name;
		this.description = description;
	}
	
	public Object getValue() {
		return this.value;
	}
	
	public String getName() {
		return this.name;
	}
	
	public String getDescription() {
		return this.description;
	}
}
