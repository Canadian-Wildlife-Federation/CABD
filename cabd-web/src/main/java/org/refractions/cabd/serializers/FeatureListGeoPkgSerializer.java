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
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureWriter;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.geopkg.Entry.DataType;
import org.geotools.geopkg.FeatureEntry;
import org.geotools.geopkg.GeoPackage;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.model.FeatureList;
import org.refractions.cabd.model.FeatureType;
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
	
	public FeatureListGeoPkgSerializer() {
		super(CabdApplication.GEOPKG_MEDIA_TYPE);
	}

	@Override
	protected void writeInternal(FeatureList features, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {

		super.writeInternal(features, outputMessage);
		
		if (features.getItems().isEmpty()) return;
		
		ImmutableTriple<String, FeatureViewMetadata, Envelope> metadataitems = FeatureListUtil.getMetadata(features, typeManager);
		
		
		SimpleFeatureType type = FeatureListUtil.asFeatureType(metadataitems.getLeft(), metadataitems.getMiddle());
		Envelope env = metadataitems.getRight();
		Path temp = Files.createTempFile("cabdgeopkg", ".gpkg");
		
		
		
		try(GeoPackage geopkg = new GeoPackage(temp.toFile())){
			
			//metadata table 
			SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
			builder.add("geometry", Geometry.class, type.getCoordinateReferenceSystem());
			builder.add("key", String.class);
			builder.add("value", String.class);
			builder.setName(FeatureListUtil.METADATA_KEY);
			builder.setCRS(type.getCoordinateReferenceSystem());
			SimpleFeatureType ftypemetadata = builder.buildFeatureType();
			
			FeatureEntry metadataentry = new FeatureEntry();
			metadataentry.setTableName(FeatureListUtil.METADATA_KEY);
			metadataentry.setDataType(DataType.Feature);
			metadataentry.setBounds(new ReferencedEnvelope(env, type.getCoordinateReferenceSystem()));
			geopkg.create(metadataentry, ftypemetadata);
			
			//features table
			FeatureEntry entry = new FeatureEntry();
			entry.setTableName("features");
			entry.setBounds(new ReferencedEnvelope(env, type.getCoordinateReferenceSystem()));
			entry.setDataType(DataType.Feature);
			geopkg.create(entry, type);
			
			try(DefaultTransaction tx = new DefaultTransaction()){

				//populate metadata table
				try(FeatureWriter<SimpleFeatureType, SimpleFeature> mwriter = geopkg.writer(metadataentry, true, Filter.INCLUDE, tx)){
					SimpleFeature feature = mwriter.next();
					feature.setAttribute("key", FeatureListUtil.DATA_LICENSE_KEY);
					feature.setAttribute("value", CabdApplication.DATA_LICENCE_URL);
					mwriter.write();
					
					feature = mwriter.next();
					feature.setAttribute("key", FeatureListUtil.DOWNLOAD_DATETIME_KEY);
					feature.setAttribute("value", FeatureListUtil.getNowAsString());
					mwriter.write();
					
					Set<String> ftypes = FeatureListUtil.getFeatureTypes(features);
					for (String ftype : ftypes) {
						FeatureType t = typeManager.getFeatureType(ftype);
						feature = mwriter.next();
						feature.setAttribute("key", t.getType() + "_" + FeatureListUtil.DATA_VERSION_KEY);
						feature.setAttribute("value", t.getDataVersion() );
						mwriter.write();
					}
					
				}
				
				//populate features table
				try(FeatureWriter<SimpleFeatureType, SimpleFeature> writer = geopkg.writer(entry, true, Filter.INCLUDE, tx)){
					FeatureListUtil.writeFeatures(writer, features, metadataitems.getMiddle(), e->e);
				}
				tx.commit();
			}
			
			outputMessage.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION, FeatureListUtil.getContentDispositionHeader(metadataitems.getLeft(), "gpkg"));
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
