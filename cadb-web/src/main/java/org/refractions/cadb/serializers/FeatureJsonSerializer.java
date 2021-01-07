package org.refractions.cadb.serializers;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;
import java.util.Map.Entry;

import org.postgresql.jdbc.PgArray;
import org.refractions.cadb.controllers.FeatureController;
import org.refractions.cadb.controllers.GeoJsonUtils;
import org.refractions.cadb.model.Feature;
import org.springframework.boot.jackson.JsonComponent;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Serializes a Feature feature as a GeoJson object.
 * 
 * @author Emily
 *
 */
@JsonComponent
public class FeatureJsonSerializer extends JsonSerializer<Feature> {

	@Override
	public void serialize(Feature value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		
		gen.writeStartObject();
		
		gen.writeStringField("type", "Feature");

		// geometry
		if (value.getGeometry() != null) GeoJsonUtils.INSTANCE.writeGeometry(gen, value.getGeometry());

		// properties
		gen.writeObjectFieldStart("properties");
		gen.writeStringField("cadb_id", value.getId().toString());
		try {
			//attributes
			for (Entry<String, Object> prop : value.getAttributes().entrySet()) {
				Object propValue = prop.getValue();
				if (propValue instanceof PgArray) {
					gen.writeFieldName(prop.getKey());
					gen.writeStartArray();
					for (Object item : (Object[])((PgArray)propValue).getArray() ) {
						gen.writeObject(item);
					}
					gen.writeEndArray();
				}else {
					gen.writeObjectField(prop.getKey(), propValue);	
				}
				
			}
			
			//links
			String rooturl = ServletUriComponentsBuilder.fromCurrentContextPath().path("/").path(FeatureController.PATH).build().toUriString();
			for (Entry<String, UUID> link : value.getLinkAttributes().entrySet()) {
				gen.writeObjectField(link.getKey(), rooturl + "/" + link.getValue().toString());	

			}
		}catch (SQLException ex) {
			throw new IOException(ex);
		}
		gen.writeEndObject();
		
		gen.writeEndObject();
	}
	
}
