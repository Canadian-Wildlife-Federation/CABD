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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.FeatureTypeListValue;
import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Abstract class for serializing feature types.
 * 
 * @author Emily
 *
 * @param <T>
 */
public abstract class AbstractFeatureTypeJsonSerializer<T> extends JsonSerializer<T> {
	
	@Override
	public abstract void serialize(T value, JsonGenerator gen, SerializerProvider serializers) throws IOException ;
	
	
	protected void serialize(FeatureType ftype, JsonGenerator gen, T value) throws IOException{
		
		gen.writeStartObject();
		
		gen.writeStringField("type", ftype.getType());

		gen.writeFieldName("attributes");
		gen.writeStartArray();
		for (FeatureViewMetadataField field : ftype.getViewMetadata().getFields()) {
			if (field.isGeometry()) continue;
			
			gen.writeStartObject();
				
			gen.writeStringField("id", field.getFieldName());
			gen.writeStringField("name", field.getName());
			gen.writeStringField("description", field.getDescription());
			gen.writeStringField("type", field.getDataType());
		
			
			if (field.getValueOptions() != null) {
				gen.writeFieldName("values");
				gen.writeStartArray();
				for (FeatureTypeListValue listitem : field.getValueOptions()) {
					gen.writeStartObject();
					gen.writeObjectField("value", listitem.getValue());
					gen.writeStringField("name", listitem.getName());
					if(listitem.getDescription() != null) {
						gen.writeStringField("description", listitem.getDescription());
					}
					if (listitem.getBbox() != null) {
						gen.writeFieldName("bbox");
						gen.writeStartArray();
						gen.writeStartArray();
						gen.writeNumber(listitem.getBbox()[0]);
						gen.writeNumber(listitem.getBbox()[1]);
						gen.writeEndArray();
						gen.writeStartArray();
						gen.writeNumber(listitem.getBbox()[2]);
						gen.writeNumber(listitem.getBbox()[3]);
						gen.writeEndArray();
						gen.writeEndArray();
						
					}
					gen.writeEndObject();
				}
				
				
				gen.writeEndArray();
			}
			
			writeOtherMetadataFieldDetails(field, value, gen);
			
			gen.writeEndObject();
		}
		
		//url attribute
		gen.writeStartObject();
		gen.writeStringField("id", FeatureViewMetadata.URL_ATTRIBUTE);
		gen.writeStringField("name", "");
		gen.writeStringField("description", "");
		gen.writeStringField("type", "url");
		gen.writeEndObject();
		
		gen.writeEndArray();//attributes
		
		gen.writeFieldName("views");
		gen.writeStartObject();
		
		DataView[] views = new DataView[]{
			new DataView("simple", a->a.getSimpleOrder()),
			new DataView("all", a->a.getAllOrder()),
		};
		
		for (DataView v : views) {
			gen.writeFieldName(v.name);
			gen.writeStartObject();
			
			gen.writeFieldName("attribute_order");
			gen.writeStartArray();
			List<FeatureViewMetadataField> temp = new ArrayList<>(ftype.getViewMetadata().getFields());
			temp = temp.stream()
				.filter(e->v.orderer.apply(e) != null)
				.sorted((a,b)->Integer.compare(v.orderer.apply(a), v.orderer.apply(b)))
				.collect(Collectors.toList());
			for (FeatureViewMetadataField f : temp) {
				gen.writeString(f.getFieldName());
			}
			gen.writeEndArray();
			gen.writeEndObject(); //simple
		}
		
		
		
		gen.writeEndObject(); //views
		
		gen.writeEndObject();
	}
	
	protected void writeOtherMetadataFieldDetails(FeatureViewMetadataField field, T value, JsonGenerator gen)  throws IOException{
		
	}
	
	
	private static class DataView{
		String name;
		Function<FeatureViewMetadataField,Integer> orderer;
		
		public DataView(String name, Function<FeatureViewMetadataField,Integer> orderer) {
			this.name = name;
			this.orderer = orderer;
		}
	
	}
}
