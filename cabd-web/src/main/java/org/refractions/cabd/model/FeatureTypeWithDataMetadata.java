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

import java.util.Map;

/**
 * Wrapper class that includes the feature type and the
 * current metadata for data in the fields.
 * 
 * @author Emily
 *
 */
public class FeatureTypeWithDataMetadata {

	private FeatureType ftype;
	private Map<FeatureViewMetadataField, FeatureViewMetadataFieldData> dataMetadata;
	
	public FeatureTypeWithDataMetadata(FeatureType ftype, Map<FeatureViewMetadataField, FeatureViewMetadataFieldData> dataMetadata) {
		this.ftype = ftype;
		this.dataMetadata = dataMetadata;
	}
	
	public FeatureType getFeatureType() {
		return this.ftype;
	}
	
	public Map<FeatureViewMetadataField, FeatureViewMetadataFieldData> getDataMetadata(){
		return this.dataMetadata; 
	}
}
