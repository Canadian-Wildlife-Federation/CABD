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

import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.controllers.GeoJsonUtils;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureList;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.stereotype.Component;

/**
 * Serializes a set of features Feature to GeoJSON
 * 
 * @author Emily
 *
 */
@Component
public class FeatureListJsonSerializer extends AbstractHttpMessageConverter<FeatureList>{


	public FeatureListJsonSerializer() {
		super(CabdApplication.GEOJSON_MEDIA_TYPE,MediaType.APPLICATION_JSON);
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
		
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		sb.append("\"type\": \"FeatureCollection\",");
		sb.append("\"features\":");
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
	
	private void writeString(OutputStream stream, String value) throws IOException {
		stream.write(value.getBytes());
	}
}
