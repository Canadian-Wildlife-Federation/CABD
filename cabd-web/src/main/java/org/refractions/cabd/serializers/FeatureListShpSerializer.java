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

import java.io.BufferedWriter;
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
 * Serializes a list of Features to shapefile (zip file containing all associated
 * files)
 * 
 * @author Emily
 *
 */
@Component
public class FeatureListShpSerializer extends AbstractFeatureListSerializer{

	@Autowired
	private FeatureTypeManager typeManager;

	private Logger logger = LoggerFactory.getLogger(FeatureListShpSerializer.class);

	public FeatureListShpSerializer() {
		super(CabdApplication.SHP_MEDIA_TYPE);
	}

	@Override
	protected void writeInternal(FeatureList features, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {
		
		super.writeInternal(features, outputMessage);
		
		if (features.getItems().isEmpty()) return;
		
		ImmutableTriple<String, FeatureViewMetadata, Envelope> metadataitems = FeatureListUtil.getMetadata(features, typeManager);
		
		FeatureViewMetadata metadata = metadataitems.getMiddle();
		
		ImmutablePair<SimpleFeatureType, Map<String,String>> typeInfo = 
				FeatureListUtil.asFeatureType(metadataitems.getLeft(), features.getAttributeSet(), 
						metadata, true);

		SimpleFeatureType type = typeInfo.getLeft();
		Map<String,String> nameMapping = typeInfo.getRight();
		
		Path temp = Files.createTempFile("cadbshp", ".shp");
		String rootFileName = FilenameUtils.getBaseName(temp.getFileName().toString());
		
		ShapefileDataStore datastore = new ShapefileDataStore(temp.toUri().toURL());
		try {
			datastore.createSchema(type);
			
			try(DefaultTransaction tx = new DefaultTransaction()){
				try(FeatureWriter<SimpleFeatureType, SimpleFeature> writer = datastore.getFeatureWriterAppend(tx)){
					FeatureListUtil.writeFeatures(writer, features, metadata, e->nameMapping.get(e));
				}
				tx.commit();
			}
		}finally {
			datastore.dispose();
		}
		List<Path> filesToZip = Files.find(temp.getParent(), 1, 
				(p,a)->FilenameUtils.getBaseName(p.getFileName().toString()).equals(rootFileName))
				.collect(Collectors.toList());
		
		//add metadata file
		Path metadatafile = temp.getParent().resolve(FeatureListUtil.METADATA_KEY + ".csv");
		try(BufferedWriter writer =  Files.newBufferedWriter(metadatafile)){
			writer.write(FeatureListUtil.DATA_LICENSE_KEY);
			writer.write(",");
			writer.write(CabdApplication.DATA_LICENCE_URL);
			writer.newLine();
			writer.write(FeatureListUtil.DOWNLOAD_DATETIME_KEY);
			writer.write(",");
			writer.write(FeatureListUtil.getNowAsString());
			writer.newLine();
			
			for (String sftype : FeatureListUtil.getFeatureTypes(features)) {
				FeatureType ft = typeManager.getFeatureType(sftype);
				writer.write(ft.getType() + "_" + FeatureListUtil.DATA_VERSION_KEY);
				writer.write(",");
				writer.write(ft.getDataVersion());
				writer.newLine();
			}
		}
		
		
		//file all files with same name as temp and zip them up
				
		String rootExportName = "cabd-" + metadataitems.getLeft();
		
		outputMessage.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION, FeatureListUtil.getContentDispositionHeader(metadataitems.getLeft(), "zip"));
		outputMessage.getHeaders().set(HttpHeaders.CONTENT_TYPE, CabdApplication.SHP_MEDIA_TYPE.getType());

		//zip file
		try(ZipOutputStream out = new ZipOutputStream(outputMessage.getBody())){
			for (Path file : filesToZip) {
				ZipEntry entry = new ZipEntry(rootExportName + "." + FilenameUtils.getExtension(file.getFileName().toString()));
				out.putNextEntry(entry);
				Files.copy(file, out);
			}
			//add metadata files
			ZipEntry entry = new ZipEntry(rootExportName + "." + FeatureListUtil.METADATA_KEY + "." + FilenameUtils.getExtension(metadatafile.getFileName().toString()));
			out.putNextEntry(entry);
			Files.copy(metadatafile, out);
		}

		for (Path p : filesToZip) {
			try {
				Files.delete(p);
			}catch (Exception ex) {
				logger.warn("Unable to delete temporary file: " + p.toString(), ex);
			}
		}
		
	}

}
