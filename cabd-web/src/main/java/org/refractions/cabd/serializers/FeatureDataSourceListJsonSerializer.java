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

import org.apache.commons.lang3.tuple.Pair;
import org.refractions.cabd.model.DataSource;
import org.refractions.cabd.model.FeatureSourceDetails;
import org.springframework.boot.jackson.JsonComponent;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Serializes a list of Feature data source fields to a json.
 * 
 * @author Emily
 *
 */
@JsonComponent
public class FeatureDataSourceListJsonSerializer extends JsonSerializer<FeatureSourceDetails> {

	@Override
	public void serialize(FeatureSourceDetails value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		
		gen.writeStartObject();
		gen.writeObjectField("cabd_id", value.getFeatureId());
		gen.writeObjectField("feature_name", value.getFeatureName());
		
		gen.writeFieldName("data_sources");
		gen.writeStartArray();
		for (DataSource ds : value.getSpatialDataSources()) {
			gen.writeStartObject();
			gen.writeObjectField("name", ds.getName());
			gen.writeObjectField("type", ds.getType());
			gen.writeObjectField("datasource_feature_id", ds.getFeatureId());
			if (value.getIncludeAllDatasourceDetails()) {
				gen.writeObjectField("version_date", ds.getVersionDate());
				gen.writeObjectField("version_number", ds.getVersion());
			}
			
			gen.writeEndObject();
		}
		for (DataSource ds : value.getNonSpatialDataSources()) {
			gen.writeStartObject();
			gen.writeObjectField("name", ds.getName());
			gen.writeObjectField("type", ds.getType());
			if (ds.getFeatureId() != null && !ds.getFeatureId().isBlank()) {
				gen.writeObjectField("datasource_feature_id", ds.getFeatureId());
			}
			if (value.getIncludeAllDatasourceDetails()) {
				gen.writeObjectField("version_date", ds.getVersionDate());
				gen.writeObjectField("version_number", ds.getVersion());
			}
			
			gen.writeEndObject();
		}
		gen.writeEndArray();
		
		gen.writeFieldName("attribute_data_sources");
		gen.writeStartObject();
		for (Pair<String,String> a : value.getAttributeDataSources()) {
			gen.writeObjectField(a.getLeft(), a.getRight());
		}
		gen.writeEndObject();
		
		gen.writeEndObject();
	}

}
