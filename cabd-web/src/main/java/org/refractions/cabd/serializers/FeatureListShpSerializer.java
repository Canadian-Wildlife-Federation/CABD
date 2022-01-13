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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureWriter;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
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
public class FeatureListShpSerializer extends AbstractHttpMessageConverter<FeatureList>{

	@Autowired
	private FeatureTypeManager typeManager;
	
	public FeatureListShpSerializer() {
		super(CabdApplication.SHP_MEDIA_TYPE);
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
		
		ImmutablePair<SimpleFeatureType, Map<String,String>> typeInfo = FeatureListUtil.asFeatureType(metadataitems.getLeft(), metadata, true);

		SimpleFeatureType type = typeInfo.getLeft();
		Map<String,String> nameMapping = typeInfo.getRight();
		
		Path temp = Files.createTempFile("cadbshp", ".shp");
		
		ShapefileDataStore datastore = new ShapefileDataStore(temp.toUri().toURL());
		datastore.createSchema(type);
		
		try(DefaultTransaction tx = new DefaultTransaction()){
			try(FeatureWriter<SimpleFeatureType, SimpleFeature> writer = datastore.getFeatureWriterAppend(tx)){
				FeatureListUtil.writeFeatures(writer, features, metadata, e->nameMapping.get(e));
			}
			tx.commit();
		}
		
		outputMessage.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=features.zip");
		outputMessage.getHeaders().set(HttpHeaders.CONTENT_TYPE, CabdApplication.SHP_MEDIA_TYPE.getType());

		//file all files with same name as temp and zip them up
		Path parent = temp.getParent();
		String rootFileName = FilenameUtils.getBaseName(temp.getFileName().toString());
		
		List<Path> filesToZip = Files.find(parent, 1, (p,a)->FilenameUtils.getBaseName(p.getFileName().toString()).equals(rootFileName)).collect(Collectors.toList());
		
		//zip file
		try(ZipOutputStream out = new ZipOutputStream(outputMessage.getBody())){
			for (Path file : filesToZip) {
				ZipEntry entry = new ZipEntry("features." + FilenameUtils.getExtension(file.getFileName().toString()));
				out.putNextEntry(entry);
				Files.copy(file, out);
			}
		}
		//TODO: delete temporary files
		
	}

}
