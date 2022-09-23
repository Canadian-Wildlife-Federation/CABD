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
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.locationtech.jts.io.WKTWriter;
import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.dao.FeatureDao;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureList;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.stereotype.Component;

import com.opencsv.CSVWriter;

/**
 * Serializes a list of Features to csv file
 * 
 * @author Emily
 *
 */
@Component
public class FeatureListCsvSerializer extends AbstractFeatureListSerializer{

	public FeatureListCsvSerializer() {
		super(CabdApplication.CSV_MEDIA_TYPE);
	}

	@Override
	protected void writeInternal(FeatureList features, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {

		super.writeInternal(features, outputMessage);
		
		if (features.getItems().isEmpty()) return ;
		
		List<String> types = features.getItems().stream().map(e->e.getFeatureType()).distinct().collect(Collectors.toList());

		
		WKTWriter wktwriter = new WKTWriter();
		
		//determine attributes; sort
		Set<String> attributes = new HashSet<>();
		for (Feature b : features.getItems()) {
			for (String key : b.getAttributes().keySet()) {
				attributes.add(key);
			}
		}
		boolean hasId = attributes.remove(FeatureDao.ID_FIELD);
		List<String> orderedAttributes = new ArrayList<>(attributes);
		Collections.sort(orderedAttributes);
		if (hasId) orderedAttributes.add(0, FeatureDao.ID_FIELD);
		
		
		String fname = FeatureListUtil.MULTI_TYPES_TYPENAME;
		if (types.size() == 1) fname = types.get(0);
		
		outputMessage.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION, FeatureListUtil.getContentDispositionHeader(fname, "csv"));
		outputMessage.getHeaders().set(HttpHeaders.CONTENT_TYPE, CabdApplication.CSV_MEDIA_TYPE_STR);
		
		try(OutputStreamWriter outwriter = new OutputStreamWriter(outputMessage.getBody());
				CSVWriter csvWriter = new CSVWriter(outwriter)){
		
			//write header
			String[] data = new String[orderedAttributes.size() + 3];
			for (int i = 0; i < orderedAttributes.size(); i ++) {
				data[i] = orderedAttributes.get(i);
			}
			data[orderedAttributes.size()] = "Geometry (WKT)";
			
			csvWriter.writeNext(data);
			
			//write each feature
			for (Feature b : features.getItems()) {
				
				for (int i = 0; i < orderedAttributes.size(); i ++) {
					Object x = b.getAttribute(orderedAttributes.get(i));
					if (x == null) {
						data[i] = null;
					}else {
						data[i] = x.toString();
					}
				}
				data[orderedAttributes.size()] = wktwriter.write(b.getGeometry());
				
				csvWriter.writeNext(data);
			}
		}
		
		
	}
}
