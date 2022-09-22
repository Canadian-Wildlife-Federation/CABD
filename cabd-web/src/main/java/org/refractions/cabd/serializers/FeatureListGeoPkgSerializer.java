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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.stereotype.Component;

/**
 * Serializes a list of Feature features geopackage file.
 * 
 * @author Emily
 *
 */
@Component
public class FeatureListGeoPkgSerializer extends AbstractFeatureListSerializer{

	@Autowired
	private FeatureTypeManager typeManager;
	
	private Logger logger = LoggerFactory.getLogger(FeatureListGeoPkgSerializer.class);

	private static final String FILENAME = "features";
	
	public FeatureListGeoPkgSerializer() {
		super(CabdApplication.GEOPKG_MEDIA_TYPE);
	}

	@Override
	protected void writeInternal(FeatureList features, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {

		super.writeInternal(features, outputMessage);
		
		if (features.getItems().isEmpty()) return;
		
		ImmutableTriple<String, FeatureViewMetadata, Envelope> metadataitems = FeatureListUtil.getMetadata(features, typeManager);
		
		FeatureViewMetadata metadata = metadataitems.getMiddle();
		
		SimpleFeatureType type = FeatureListUtil.asFeatureType(metadataitems.getLeft(), metadata);
		Envelope env = metadataitems.getRight();
		
		Path temp = Files.createTempFile("cabdgeopkg", ".gpkg");
		
		try(GeoPackage geopkg = new GeoPackage(temp.toFile())){
			
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
			
			outputMessage.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=" + FILENAME + ".gpkg");
			outputMessage.getHeaders().set(HttpHeaders.CONTENT_TYPE, CabdApplication.GEOPKG_MEDIA_TYPE_STR);
	
			Files.copy(temp, outputMessage.getBody());
			outputMessage.getBody().flush();
		}

		try {
			Files.delete(temp);
		}catch (Exception ex) {
			logger.warn("Unable to delete temporary file: " + temp.toString(), ex);
		}
	}

}
