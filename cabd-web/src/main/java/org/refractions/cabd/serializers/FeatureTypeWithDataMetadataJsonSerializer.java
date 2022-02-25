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
package org.refractions.cabd.serializers;

import java.io.IOException;
import java.util.Map.Entry;

import org.refractions.cabd.model.FeatureTypeWithDataMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;
import org.refractions.cabd.model.FeatureViewMetadataFieldData;
import org.refractions.cabd.model.FeatureViewMetadataFieldData.MetadataValue;
import org.springframework.boot.jackson.JsonComponent;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Serializer for a single feature type that contains additional data metadata
 * 
 * @author Emily
 *
 */
@JsonComponent
public class FeatureTypeWithDataMetadataJsonSerializer extends AbstractFeatureTypeJsonSerializer<FeatureTypeWithDataMetadata> {

	@Override
	public void serialize(FeatureTypeWithDataMetadata value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		serialize(value.getFeatureType(), gen, value);
	}		
	
	
	protected void writeOtherMetadataFieldDetails(FeatureViewMetadataField field, FeatureTypeWithDataMetadata value, JsonGenerator gen) throws IOException {
		FeatureViewMetadataFieldData datamd = value.getDataMetadata().get(field);
		if (datamd != null) {
			for (Entry<MetadataValue, Object> entry : datamd.getData().entrySet()) {
				gen.writeObjectField(entry.getKey().key, entry.getValue());
			}
		}
	}
	
}
