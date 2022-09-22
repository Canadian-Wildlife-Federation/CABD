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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.geotools.data.FeatureWriter;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.refractions.cabd.dao.FeatureDao;
import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureList;
import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

/**
 * Utilities for feature lists, primarily for converting
 * feature lists into geotools feature collections.
 *  
 * @author Emily
 *
 */
public class FeatureListUtil {

	public static final String MULTI_TYPES_TYPENAME = "features";
	
	/**
	 * For determining filenames for export formats that require content
	 * disposition header
	 * @param ftype feature type as string (used for filename)
	 * @param extension format extension
	 * @return
	 */
	public static String getContentDispositionHeader(String ftype, String extension) {
		return "attachment;filename=cabd-" + ftype + "." + extension;
	}
	
	public static ImmutableTriple<String, FeatureViewMetadata, Envelope> getMetadata(FeatureList features, FeatureTypeManager typeManager) throws IOException{

		Set<String> ftypes = new HashSet<>();
		Envelope env = null;
		for (Feature feature : features.getItems()) {
			ftypes.add(feature.getFeatureType());
			if (env == null) {
				env = feature.getGeometry().getEnvelopeInternal();
			}else {
				env.expandToInclude(feature.getGeometry().getEnvelopeInternal());
			}
		}
		
		FeatureViewMetadata metadata = null;
		String barriertype = MULTI_TYPES_TYPENAME;
		if (ftypes.size() == 1) {
			//create a schema specific to the feature type
			barriertype = ftypes.iterator().next();
			
			if (!typeManager.isValidType(barriertype)) {
				throw new IOException("Feature type not supported");
			}
			metadata = typeManager.getFeatureType(barriertype).getViewMetadata();
		}else {
			//create a schema generic to all features
			metadata = typeManager.getAllViewMetadata();
		}
		return  new ImmutableTriple<>(barriertype, metadata, env);
	}
	
	public static SimpleFeatureType asFeatureType(String featureType, FeatureViewMetadata metadata) throws IOException{
		return asFeatureType(featureType, metadata, false).getLeft();
	}
	
	public static ImmutablePair<SimpleFeatureType, Map<String,String>> asFeatureType(String featureType, FeatureViewMetadata metadata, boolean forshape) throws IOException{
		
		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		Set<String> names = new HashSet<>();
		HashMap<String,String> nameMapping = new HashMap<>();
		for (FeatureViewMetadataField field : metadata.getFields()) {
			String fieldName = field.getFieldName();
			if (forshape) {
				String old = fieldName;
				fieldName = computeFieldName(fieldName, names);
				nameMapping.put(old, fieldName);
				names.add(fieldName);
			}
			if (field.isGeometry()) {
				builder.add(fieldName, Point.class, FeatureDao.DATABASE_SRID);
				builder.setDefaultGeometry(fieldName);
			}else {
				builder.add(fieldName, field.getDataTypeAsClass());
			}
		}
		builder.setName(featureType);
		SimpleFeatureType type = builder.buildFeatureType();
		return new ImmutablePair<>(type,nameMapping);
	}
	
	/*
	 * truncates field names to 10 characters for shapefiles
	 * also removes all _ characters in names longer than 10 chars.
	 */
	private static String computeFieldName(String name, Set<String> current) {
		if (name.length() <= 10) return name;
		
		name = name.replaceAll("_", "");
		if (name.length() <= 10 && !current.contains(name)) return name;
		
		String fname = name.substring(0, 10);
		if (current.contains(fname)) {
			int i = 1;
			while(current.contains(fname)) {
				String index = String.valueOf(i);
				int end = 10-index.length();
				fname = name.substring(0, end)+ index;
				i++;
			}
		}
		return fname;
	}
	
	public static void writeFeatures(FeatureWriter<SimpleFeatureType, SimpleFeature> writer, FeatureList features, 
			FeatureViewMetadata metadata, Function<String, String> attributeNameMapper ) throws IOException{
		//create features
		String rooturl = ServletUriComponentsBuilder.fromCurrentContextPath().path("/").build().toUriString();

		for (Feature f : features.getItems()) {
			SimpleFeature sfeature = writer.next();
			for (FeatureViewMetadataField field : metadata.getFields()) {
				if (field.isGeometry()) {
					sfeature.setDefaultGeometry(f.getGeometry());
				}else if (f.getAttributes().containsKey(field.getFieldName())) {
					Object value = f.getAttribute(field.getFieldName());
					sfeature.setAttribute(attributeNameMapper.apply(field.getFieldName()), value);
				}else if (f.getLinkAttributes().containsKey(field.getFieldName())) {
					String value = f.getLinkAttributes().get(field.getFieldName());
					if (value != null) {
						sfeature.setAttribute(attributeNameMapper.apply(field.getFieldName()), rooturl + value);
					}else {
						sfeature.setAttribute(attributeNameMapper.apply(field.getFieldName()), null);
					}
				}
			}
			writer.write();
		}
		
	}
	
	
}
