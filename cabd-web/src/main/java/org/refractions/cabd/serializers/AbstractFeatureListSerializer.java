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

import org.refractions.cabd.model.FeatureList;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

/**
 * Serializer for list of features
 * 
 * @author Emily
 *
 */
public abstract class AbstractFeatureListSerializer extends AbstractHttpMessageConverter<FeatureList>{

	/**
	 * 
	 * @param supportedMediaTypes the supported media types
	 */
	protected AbstractFeatureListSerializer(MediaType... supportedMediaTypes) {
		super(supportedMediaTypes);
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

		outputMessage.getHeaders().set(HttpHeaders.CONTENT_RANGE, "features 0-" + features.getItems().size() + "/" + features.getTotalResults());
		
	}
}