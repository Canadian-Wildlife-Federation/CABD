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

	public List<Feature> features;
	
	public FeatureList(List<Feature> features) {
		this.features = features;		
		String rooturl = ServletUriComponentsBuilder.fromCurrentContextPath().path("/").path(FeatureController.PATH).build().toUriString();		
		features.forEach(f->f.addAttribute("url", rooturl + "/" + f.getId().toString()));
	}
	
	public List<Feature> getFeatures(){
		return this.features;
	}
}
