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
import java.io.OutputStream;
import java.util.Set;

import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.controllers.GeoJsonUtils;
import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureList;
import org.refractions.cabd.model.FeatureType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.stereotype.Component;

/**
 * Serializes a set of features Feature to GeoJSON
 * 
 * @author Emily
 *
 */
@Component
public class FeatureListJsonSerializer extends AbstractFeatureListSerializer{

	@Autowired
	private FeatureTypeManager typeManager;
	
	public FeatureListJsonSerializer() {
		super(CabdApplication.GEOJSON_MEDIA_TYPE,MediaType.APPLICATION_JSON);
	}
	
	@Override
	protected void writeInternal(FeatureList features, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {
		
		super.writeInternal(features, outputMessage);
		
		
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		sb.append(formatString("type") + ":" + formatString("FeatureCollection") + ",");
		sb.append(formatString(FeatureListUtil.METADATA_KEY) + ": {"); 
		sb.append(formatString(FeatureListUtil.DOWNLOAD_DATETIME_KEY ) + ": "+ formatString(FeatureListUtil.getNowAsString()) + ",");
		sb.append(formatString(FeatureListUtil.DATA_LICENSE_KEY) + ": "+ formatString(CabdApplication.DATA_LICENCE_URL) + ",");
		
		Set<String> ftypes = FeatureListUtil.getFeatureTypes(features);
		if (!ftypes.isEmpty()) {
			sb.append(formatString(FeatureListUtil.DATA_VERSION_KEY) + ": {");
			for (String ftype : ftypes) {
				FeatureType type = typeManager.getFeatureType(ftype) ;
				sb.append(formatString(type.getType())  + ": " + formatString(type.getDataVersion()) + ",");
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append("},");
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append("}, ");
		sb.append(formatString("features") + ":");
		sb.append("[");
		
		writeString(outputMessage.getBody(), sb.toString());
		
		boolean first = true;
		for (Feature b : features.getItems()) {
			if (!first) writeString(outputMessage.getBody(),",");
			GeoJsonUtils.INSTANCE.writeFeature(b, outputMessage.getBody());
			first = false;
		}
		writeString(outputMessage.getBody(), "]}");
		
	}
	private String formatString(String value) {
		return "\"" + value  + "\"";
	}
	
	private void writeString(OutputStream stream, String value) throws IOException {
		stream.write(value.getBytes());
	}
}
