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

import java.util.List;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.refractions.cabd.dao.filter.Filter;
import org.refractions.cabd.model.FeatureType;

public class ParsedRequestParameters {

	private Integer maxResults;
	private Envelope env = null;
	private Coordinate searchPoint = null;
	private List<FeatureType> ftypes = null;
	private Filter filter;
	
	public ParsedRequestParameters(Envelope env, Coordinate searchPoint, Integer maxResults, Filter filter) {
		this.env  = env;
		this.searchPoint = searchPoint;
		this.maxResults = maxResults;
		this.filter = filter;
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

	public Filter getFilter() {
		return this.filter;
	}
}
