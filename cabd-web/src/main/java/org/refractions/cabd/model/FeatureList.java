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
package org.refractions.cabd.model;

import java.util.List;

import org.refractions.cabd.controllers.FeatureController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

/**
 * A collection of Features.
 * 
 * @author Emily
 *
 */
//I created this class to make it simple to
//serialize a list of features to a GeoJson
//feature collection
public class FeatureList {

	private List<Feature> features;
	
	public FeatureList(List<Feature> features) {
		this.features = features;		
		String rooturl = ServletUriComponentsBuilder.fromCurrentContextPath().path("/").path(FeatureController.PATH).build().toUriString();		
		features.forEach(f->f.addAttribute(FeatureViewMetadata.URL_ATTRIBUTE, rooturl + "/" + f.getId().toString()));
	}
	
	public List<Feature> getFeatures(){
		return this.features;
	}
}
