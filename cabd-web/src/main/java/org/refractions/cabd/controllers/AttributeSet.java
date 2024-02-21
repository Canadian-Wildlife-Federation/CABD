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
package org.refractions.cabd.controllers;

/**
 * Enum to represent which set of attributes
 * are to be returned by the JSON api request.
 * 
 * @author Emily
 *
 */
public class AttributeSet {
	
	
	public static final String VECTOR_TILE = "vectortile";
	
	private String name;
	private String column;
	
	public AttributeSet(String name, String column) {
		this.name = name.toLowerCase().trim();
		this.column = column;
	}
	
	public String getName() {
		return this.name;
	}
	
	public String getColumn() {
		return this.column;
	}
	
}
