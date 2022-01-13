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
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureWriter;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.geopkg.Entry.DataType;
import org.geotools.geopkg.FeatureEntry;
import org.geotools.geopkg.GeoPackage;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.model.FeatureList;
import org.refractions.cabd.model.FeatureViewMetadata;
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
public class FeatureListGeoPkgSerializer extends AbstractHttpMessageConverter<FeatureList>{

	@Autowired
	private FeatureTypeManager typeManager;
	
	public FeatureListGeoPkgSerializer() {
		super(CabdApplication.GEOPKG_MEDIA_TYPE);
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
		Envelope env = metadataitems.getRight();
		
		Path temp = Files.createTempFile("cabdgeopkg", ".gpkg");
		GeoPackage geopkg = new GeoPackage(temp.toFile());
		
		FeatureEntry entry = new FeatureEntry();
		entry.setTableName("features");
		entry.setBounds(new ReferencedEnvelope(env, type.getCoordinateReferenceSystem()));
		entry.setDataType(DataType.Feature);
		geopkg.create(entry, type);
		
		try(DefaultTransaction tx = new DefaultTransaction()){
			try(FeatureWriter<SimpleFeatureType, SimpleFeature> writer = geopkg.writer(entry, true, Filter.INCLUDE, tx)){
				FeatureListUtil.writeFeatures(writer, features, metadata, e->e);
			}
			tx.commit();
		}
		
		outputMessage.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=features.gpkg");
		outputMessage.getHeaders().set(HttpHeaders.CONTENT_TYPE, CabdApplication.GEOPKG_MEDIA_TYPE_STR);

		Files.copy(temp, outputMessage.getBody());
		//TODO: delete temporary file
		
	}

}