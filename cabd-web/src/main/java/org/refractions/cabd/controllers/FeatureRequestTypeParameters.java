package org.refractions.cabd.controllers;

import java.beans.ConstructorProperties;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.exceptions.InvalidParameterException;
import org.refractions.cabd.model.FeatureType;

import io.swagger.v3.oas.annotations.Parameter;

/**
 * Extension of the FeatureRequestParameters that includes an option
 * types parameter.
 * 
 * @author Emily
 *
 */
public class FeatureRequestTypeParameters extends FeatureRequestParameters{

	//valid query parameters
	@Parameter(required = false, description = "A comma delimited list of feature types to search.") 
	private String types;
	
	
	//this is the only way I could figure out
	//how to provide names for query parameters and
	//use a POJO to represent these parameters
	//I needed custom name for max-results
	//https://stackoverflow.com/questions/56468760/how-to-collect-all-fields-annotated-with-requestparam-into-one-object
	@ConstructorProperties({"types","bbox", "point","max-results"})
	public FeatureRequestTypeParameters(
			String types, String bbox, 
			String point, Integer maxResults) {
		super(bbox, point, maxResults);
		this.types = types;
	}
		
	public String getTypes() { return this.types; }

	
	/**
	 * Parses parameters into data types and
	 * validates values.
	 * 
	 * @param typeManager
	 */
	//TODO: figure out how  we can autowrite type manager; 
	public ParsedRequestParameters parseAndValidate(FeatureTypeManager typeManager) {
		ParsedRequestParameters bb = super.parseAndValidate();
		if (this.types != null) {
			String[] ttypes = this.types.split(",");
			bb.setFeatureTypes( parseTypes(ttypes, typeManager));
		}
		return bb;
	}
	
	private List<FeatureType> parseTypes(String[] strtypes, FeatureTypeManager typeManager){
		List<FeatureType> types = new ArrayList<>();
		for (String stype : strtypes) {
			FeatureType type = typeManager.getFeatureType(stype);
			if (type == null) {
				throw new InvalidParameterException(MessageFormat.format("The feature type {0} is not registered in the system.",  stype));
			}
			types.add(type);
		}
		return types;
	}
	
	
}
