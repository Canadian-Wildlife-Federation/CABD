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
import java.util.Collections;

import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.model.CrsInfo;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.stereotype.Component;

/**
 * Serializes a single Feature to kml
 * 
 * @author Emily
 *
 */
@Component
public class FeatureKmlSerializer extends AbstractHttpMessageConverter<Feature>{

	@Autowired
	private FeatureListKmlSerializer listSerializer;
	
	public FeatureKmlSerializer() {
		super(CabdApplication.KML_MEDIA_TYPE);
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
		CrsInfo info = feature.getCrsInfo();
		if (info == null) info = new CrsInfo("EPSG", 4326, 4326);
		FeatureList fl = new FeatureList(Collections.singletonList(feature), null, info);
		listSerializer.writeInternal(fl, outputMessage);
	}
	
	

}
