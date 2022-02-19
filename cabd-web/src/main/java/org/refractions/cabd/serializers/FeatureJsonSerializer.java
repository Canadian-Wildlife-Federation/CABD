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

import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.controllers.GeoJsonUtils;
import org.refractions.cabd.model.Feature;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.stereotype.Component;

/**
 * Serializes a single Feature to GeoJSON
 * 
 * @author Emily
 *
 */
@Component
public class FeatureJsonSerializer extends AbstractHttpMessageConverter<Feature>{


	public FeatureJsonSerializer() {
		super(CabdApplication.GEOJSON_MEDIA_TYPE,MediaType.APPLICATION_JSON);
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return Feature.class.isAssignableFrom(clazz);
	}

	@Override
	protected Feature readInternal(Class<? extends Feature> clazz, HttpInputMessage inputMessage)
			throws IOException, HttpMessageNotReadableException {
		return null;
	}

	@Override
	protected void writeInternal(Feature feature, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {
	
		GeoJsonUtils.INSTANCE.writeFeature(feature, outputMessage.getBody());
	}
	
	

}
