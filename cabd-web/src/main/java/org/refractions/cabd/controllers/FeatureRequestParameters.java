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
package org.refractions.cabd.controllers;

import java.beans.ConstructorProperties;
import java.text.MessageFormat;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.refractions.cabd.dao.FeatureDao;
import org.refractions.cabd.dao.filter.Filter;
import org.refractions.cabd.exceptions.InvalidParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.swagger.v3.oas.annotations.Parameter;

/**
 * Class to represent query parameters for querying
 * features.
 * 
 * @author Emily
 *
 */
public class FeatureRequestParameters {

	private Logger logger = LoggerFactory.getLogger(FeatureDao.class);

	@Parameter(required = false, description = "A bounding box in lat/long to search.  Should be of the form: 'xmin,ymin,xmax,ymax'")
	private String bbox;
	
	@Parameter(required = false, description = "A search point to search for nearest features.  Should be of the form: 'longitude,latitude'. Will be ignored if bbox is provided  ")
	private String point;
	
	@Parameter(name="max-results", required = false, description = "The maximum number of search results to return.  Required if point is provided.  If not provided a system defined maximum is used.")
	private Integer maxresults;

	@Parameter(name="filter", required = false, description = "The feature filter.")
	private String[] filter;
	
	@Parameter(name="attributes", required = false, description = "A flag to set if a complete set or limitted set of attributes should be returned.")
	private AttributeSet attributes;
	
	//this is the only way I could figure out
	//how to provide names for query parameters and
	//use a POJO to represent these parameters
	//I needed custom name for max-results
	//https://stackoverflow.com/questions/56468760/how-to-collect-all-fields-annotated-with-requestparam-into-one-object
	@ConstructorProperties({"bbox", "point","max-results", "filter", "attributes"})
	public FeatureRequestParameters(
			String bbox, 
			String point, Integer maxResults, String[] filter, String attributes) {

		this.bbox = bbox;
		this.point = point;
	    this.maxresults = maxResults;
	    this.filter = filter;
	    this.attributes = AttributeSet.parse(attributes);
	}
	
	public String getBbox() { return this.bbox; }
	public String getPoint() { return this.point; }
	public Integer getMaxresults() { return maxresults;	}
	public String[] getFilter() { return filter;	}
	public AttributeSet getAttributeSet() { return this.attributes; }

	
	/**
	 * Parses parameters into data types and
	 * validates values.
	 * 
	 * @param typeManager
	 */
	public ParsedRequestParameters parseAndValidate() {
		Envelope env = null;
		if (bbox != null) {
			env = parseBbox();
		}

		Coordinate searchPoint = null;
		if (point != null) {
			String bits[] = point.split(",");
			if (bits.length != 2) {
				throw new InvalidParameterException("The point parameter is invalid. Must be of the form: point=<x>,<y>");
			}

			double x = 0;
			double y = 0;
			try {
				x = Double.parseDouble(bits[0]);
				y = Double.parseDouble(bits[1]);
			}catch (Exception ex) {
				logger.warn(ex.getMessage(), ex);
				throw new InvalidParameterException("The point parameter is invalid. Must be of the form: point=<x>,<y>");
			}
			
			validateCoordinate(x, y);
			searchPoint = new Coordinate(x,y);
			
			
			if (maxresults == null) {
				throw new InvalidParameterException("The 'max-results' parameter must be supplied with the 'point' parameter is used.");
			}
			if (maxresults < 0) {
				throw new InvalidParameterException("The 'max-results' parameter must be larger than 0");
			}
		}
		return new ParsedRequestParameters(env, searchPoint, maxresults, parseFilter(filter), attributes);
	}

	private Envelope parseBbox() {
		String[] parts = bbox.split(",");
		
		if (parts.length != 4) {
			throw new InvalidParameterException("The bbox parameter is invalid. Must be of the form: bbox=<xmin>,<ymin>,<xmax>,<ymax>");
		}
		try {
			double xmin = Double.parseDouble(parts[0]);
			double ymin = Double.parseDouble(parts[1]);
			double xmax = Double.parseDouble(parts[2]);
			double ymax = Double.parseDouble(parts[3]);
			
			if (xmin > xmax) {
				double temp = xmin;
				xmin = xmax;
				xmax = temp;
			}
			if (ymin > ymax) {
				double temp = ymin;
				ymin = ymax;
				ymax = temp;
			}
			
			validateCoordinate(xmin, ymin);
			validateCoordinate(xmax, ymax);

			return new Envelope(xmin, xmax, ymin, ymax);
		}catch (Exception ex) {
			logger.warn(ex.getMessage(), ex);
			throw new InvalidParameterException("The bbox parameter is invalid. Must be of the form: bbox=<xmin>,<ymin>,<xmax>,<ymax>");
		}
	}
	
	
	private void validateCoordinate(double x, double y) {
		//ensure x,y is within the bounds of web mercator
		if (!FeatureDao.VALID_BOUNDS.contains(new Coordinate(x,y))) {
			throw new InvalidParameterException(MessageFormat.format("The coordinates provided ({0}, {1}) are outside the valid bounds ({2},{3},{4},{5})",  x,y,FeatureDao.VALID_BOUNDS.getMinX(),FeatureDao.VALID_BOUNDS.getMinY(),FeatureDao.VALID_BOUNDS.getMaxX(),FeatureDao.VALID_BOUNDS.getMaxY()));
		}
	}
	
	private Filter parseFilter(String[] filters) {
		if (filters == null || filters.length == 0) return null;
		return Filter.parseFilter(filters);
	}
	
}
