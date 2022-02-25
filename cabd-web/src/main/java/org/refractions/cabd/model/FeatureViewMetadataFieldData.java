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

import java.util.HashMap;
import java.util.Map;

/**
 * Metadata about the current feature data for
 * a given metadata field (min, max values). 
 * 
 * @author Emily
 *
 */
public class FeatureViewMetadataFieldData {

	public enum MetadataValue{
		MIN("min_value"),
		MAX("max_value");
		
		public String key;
		
		MetadataValue(String key){
			this.key = key;
		}
	}
	private FeatureViewMetadataField field;
	
	private Map<MetadataValue, Object> data;
	
	public FeatureViewMetadataFieldData(FeatureViewMetadataField field) {
		this.field = field;
		data = new HashMap<>();
	}
	
	public FeatureViewMetadataField getField() {
		return field;
	}
	public void addData(MetadataValue field, Object value) {
		data.put(field,  value);
	}
	
	public Map<MetadataValue, Object> getData(){
		return this.data;
	}
}
