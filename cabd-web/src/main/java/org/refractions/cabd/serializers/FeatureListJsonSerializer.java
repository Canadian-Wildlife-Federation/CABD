package org.refractions.cabd.serializers;

import java.io.IOException;

import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureList;
import org.springframework.boot.jackson.JsonComponent;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Serializes a list of Feature features to a GeoJson FeatureCollection.
 * 
 * @author Emily
 *
 */
@JsonComponent
public class FeatureListJsonSerializer extends JsonSerializer<FeatureList> {

	@Override
	public void serialize(FeatureList value, JsonGenerator gen, SerializerProvider serializers) throws IOException {

		gen.writeStartObject();
		gen.writeStringField("type", "FeatureCollection");

		// features
		gen.writeFieldName("features");
		gen.writeStartArray();
		for (Feature b : value.getFeatures()) {
			gen.writeObject(b);
		}
		gen.writeEndArray();
		
		gen.writeEndObject();

	}

	

}
