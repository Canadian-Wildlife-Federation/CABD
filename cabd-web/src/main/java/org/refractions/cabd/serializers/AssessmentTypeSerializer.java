/*
 * Copyright 2025 Canadian Wildlife Federation
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

import org.refractions.cabd.model.assessment.AssessmentType;
import org.refractions.cabd.model.assessment.AssessmentTypeMetadataField;
import org.springframework.boot.jackson.JsonComponent;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

@JsonComponent
public class AssessmentTypeSerializer extends JsonSerializer<AssessmentType> {
	
	
	@Override
	public void serialize(AssessmentType atype, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		
		gen.writeStartObject();
		
		gen.writeStringField("type", atype.getType());

		gen.writeFieldName("attributes");
		gen.writeStartArray();
		for (AssessmentTypeMetadataField field : atype.getSiteAttributes()) {
			
			gen.writeStartObject();
				
			gen.writeStringField("id", field.getFieldName());
			gen.writeStringField("name", field.getName());
			gen.writeStringField("description", field.getDescription());
			gen.writeStringField("type", field.getDataType());
		
			gen.writeEndObject();
		}
		gen.writeEndArray();		
		
		gen.writeFieldName("structure_attributes");
		gen.writeStartArray();
		for (AssessmentTypeMetadataField field : atype.getStructureAttributes()) {
			
			gen.writeStartObject();
				
			gen.writeStringField("id", field.getFieldName());
			gen.writeStringField("name", field.getName());
			gen.writeStringField("description", field.getDescription());
			gen.writeStringField("type", field.getDataType());
		
			gen.writeEndObject();
		}
		gen.writeEndArray();		
		gen.writeEndObject();
	}

}
