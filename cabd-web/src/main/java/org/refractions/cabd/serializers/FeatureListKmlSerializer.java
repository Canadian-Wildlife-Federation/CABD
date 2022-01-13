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

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.xsd.Encoder;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureList;
import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.stereotype.Component;

/**
 * Serializes a list of Feature features to a GeoJson FeatureCollection.
 * 
 * @author Emily
 *
 */
@Component
public class FeatureListKmlSerializer extends AbstractHttpMessageConverter<FeatureList>{

	@Autowired
	private FeatureTypeManager typeManager;
	
	public FeatureListKmlSerializer() {
		super(CabdApplication.KML_MEDIA_TYPE);
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return FeatureList.class.isAssignableFrom(clazz);
	}


	@Override
	protected FeatureList readInternal(Class<? extends FeatureList> clazz, HttpInputMessage inputMessage)
			throws IOException, HttpMessageNotReadableException {
		return null;
	}

	@Override
	protected void writeInternal(FeatureList features, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {
	
		if (features.getFeatures().isEmpty()) return;
		
		ImmutableTriple<String, FeatureViewMetadata, Envelope> metadataitems = FeatureListUtil.getMetadata(features, typeManager);
		
		FeatureViewMetadata metadata = metadataitems.getMiddle();
		
		SimpleFeatureType type = FeatureListUtil.asFeatureType(metadataitems.getLeft(), metadata);
		
		ListFeatureCollection cfeatures = new ListFeatureCollection(type);
		
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(type);
		
		for (Feature f : features.getFeatures()) {
			for (FeatureViewMetadataField field : metadata.getFields()) {
				Object value = f.getAttribute(field.getFieldName());
				if (field.isGeometry()) {
					value = f.getGeometry();
				}
				builder.set(field.getFieldName(), value);
			}
			SimpleFeature sf = builder.buildFeature(f.getId().toString());
			cfeatures.add(sf);
		}
		
		outputMessage.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=features.kml");
		outputMessage.getHeaders().set(HttpHeaders.CONTENT_TYPE, CabdApplication.KML_MEDIA_TYPE.getType());
		
		Encoder encoder = new Encoder(new org.geotools.kml.v22.KMLConfiguration());
		encoder.encode(cfeatures, org.geotools.kml.v22.KML.kml, outputMessage.getBody());
		
		//TODO: memory constraints
		
	}

}
