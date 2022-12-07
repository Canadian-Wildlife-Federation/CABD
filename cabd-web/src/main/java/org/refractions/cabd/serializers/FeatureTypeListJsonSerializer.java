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
package org.refractions.cabd.serializers;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.FeatureViewMetadataField;
import org.springframework.boot.jackson.JsonComponent;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Serializer for a list feature types
 * 
 * @author Emily
 *
 */
@JsonComponent
public class FeatureTypeListJsonSerializer extends JsonSerializer<List<FeatureType>> {

	@Override
	public void serialize(List<FeatureType> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {

		gen.writeStartArray();
		for (FeatureType type: value) {
			gen.writeStartObject();
			gen.writeStringField("type", type.getType());
			gen.writeStringField("name", type.getName());
			gen.writeStringField("metadata", type.getMetadataUrl());
			gen.writeStringField("features", type.getDataUrl());
			gen.writeStringField("data_version", type.getDataVersion());
			gen.writeEndObject();
		}
		gen.writeEndArray();
	}
		
	class DataView{
		String name;
		Function<FeatureViewMetadataField,Integer> orderer;
		
		public DataView(String name, Function<FeatureViewMetadataField,Integer> orderer) {
			this.name = name;
			this.orderer = orderer;
		}
	
	}
	
}
