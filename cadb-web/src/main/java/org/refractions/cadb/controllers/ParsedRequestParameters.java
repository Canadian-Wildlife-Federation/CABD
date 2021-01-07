package org.refractions.cadb.controllers;

import java.util.List;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.refractions.cadb.model.FeatureType;

public class ParsedRequestParameters {

	private Integer maxResults;
	private Envelope env = null;
	private Coordinate searchPoint = null;
	private List<FeatureType> ftypes = null;
	
	public ParsedRequestParameters(Envelope env, Coordinate searchPoint, Integer maxResults) {
		this.env  = env;
		this.searchPoint = searchPoint;
		this.maxResults = maxResults;
	}
	
	public ParsedRequestParameters(List<FeatureType> types, Envelope env, Coordinate searchPoint, Integer maxResults) {
		this.ftypes = types;
		this.env  = env;
		this.searchPoint = searchPoint;
		this.maxResults = maxResults;
	}
	
	public Integer getMaxResults() {
		return maxResults;
	}

	public Envelope getEnvelope() {
		return env;
	}

	public Coordinate getSearchPoint() {
		return searchPoint;
	}

	public List<FeatureType> getFeatureTypes() {
		return ftypes;
	}
	
	public void setFeatureTypes(List<FeatureType> types) {
		this.ftypes = types;
	}

	
	
}
