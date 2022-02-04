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

import java.beans.ConstructorProperties;

import io.swagger.v3.oas.annotations.Parameter;

/**
 * Class to represent query parameters for querying
 * features data sources
 * 
 * @author Emily
 *
 */
public class FeatureDataSourceRequestParameters {

	@Parameter(required = false, description = "Identifies what fields to include in the results. 'all' - includes all fields otherwise limited fields are included")
	private String fields;
		
	@ConstructorProperties({"fields"})
	public FeatureDataSourceRequestParameters(String fields) {
		this.fields = fields;
	}
	
	public String getFields() { return this.fields; }

	
}
