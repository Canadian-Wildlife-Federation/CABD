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
import java.text.DateFormat;
import java.util.Date;

import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.model.FeatureDataSourceDetails;
import org.refractions.cabd.model.FeatureDataSourceList;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;

/**
 * Serializes a list of Feature data source items into csv format.
 * 
 * @author Emily
 *
 */
@Component
public class FeatureDataSourceListCsvSerializer extends AbstractHttpMessageConverter<FeatureDataSourceList>{

	
	public FeatureDataSourceListCsvSerializer() {
		super(CabdApplication.CSV_MEDIA_TYPE);
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return FeatureDataSourceList.class.isAssignableFrom(clazz);
	}


	@Override
	protected FeatureDataSourceList readInternal(Class<? extends FeatureDataSourceList> clazz, HttpInputMessage inputMessage)
			throws IOException, HttpMessageNotReadableException {
		return null;
	}

	private String convertToString(String value) {
		if (value == null) return "";
		return value;
	}
	private String convertToString(Date value) {
		if (value == null) return "";
		return DateFormat.getDateInstance().format(value);
	}
	
	@Override
	protected void writeInternal(FeatureDataSourceList features, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {
		
		//outputMessage.getHeaders().set(HttpHeaders.CONTENT_TYPE, "text/plain");
		CsvMapper csvwriter = 
				CsvMapper.builder().enable(CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING)
				.build();
		try(SequenceWriter seqW = csvwriter.writer().writeValues(outputMessage.getBody())){
		
			if (features.getIncludeAllAttributes()) {
				seqW.write(new String[] {"cabd_id",
						"attribute_field_name","attribute_name",
						"datasource_name","datasource_date","datasource_version",
						"datasource_feature_id"});
				for (FeatureDataSourceDetails details: features.getItems()) {
					seqW.write(new String[] {	
							details.getCabdId().toString(),
							convertToString(details.getAttributeFieldName()),
							convertToString(details.getAttributeName()),
							convertToString(details.getDsName()),
							convertToString(details.getDsDate()),
							convertToString(details.getDsVersion()),
							convertToString(details.getDsFeatureId())});
				}
				
			}else {
				seqW.write(new String[] {"cabd_id","attribute_field_name","datasource_name","datasource_feature_id"});
				for (FeatureDataSourceDetails details: features.getItems()) {
					seqW.write(new String[] {	
							details.getCabdId().toString(),
							convertToString(details.getAttributeFieldName()),
							convertToString(details.getDsName()),
							convertToString(details.getDsFeatureId())});
				}
			}
			
		}
		
	}

}
