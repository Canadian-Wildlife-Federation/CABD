/*
 * Copyright 2026 Canadian Wildlife Federation
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
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

import io.swagger.v3.oas.annotations.Parameter;

/**
 * Class to represent query parameters for querying community
 * features.
 * 
 * @author Emily
 *
 */
public class CommunityRequestParameters extends CrsRequestParameters {

	@Parameter(name="fromdate", required = false, description = "Only returns records uploaded on or after this date. Expects  ISO 8601 date or datetime. Defaults to one year ago.")
	private OffsetDateTime fromdate;
	
	@Parameter(name="todate", required = false, description = "Only returns records uploaded on or before this date. Expects  ISO 8601 date or datetime")
	private OffsetDateTime todate;
	
	@Parameter(name="max-results", required = false, description = "The maximum number of search results to return. If not provided a system defined maximum is used.")
	private Integer maxresults;
	
	//this is the only way I could figure out
	//how to provide names for query parameters and
	//use a POJO to represent these parameters
	//I needed custom name for max-results
	//https://stackoverflow.com/questions/56468760/how-to-collect-all-fields-annotated-with-requestparam-into-one-object
	@ConstructorProperties({ "fromdate","todate","max-results", "crs"})
	public CommunityRequestParameters(String fromdate, String todate,Integer maxResults, String crs) {
		super(crs);
		this.fromdate = parseDateTime(fromdate);
		this.todate = parseDateTime(todate);
		this.maxresults = maxResults;
	}

	public Integer getMaxresults() { return maxresults;	}
	public OffsetDateTime getFromDate() { return fromdate; }
	public OffsetDateTime getToDate() { return todate; }

	/**
	 * Parses parameters into data types and
	 * validates values.
	 * 
	 * @param typeManager
	 */	
	private OffsetDateTime parseDateTime(String datetime) {
		if (datetime == null || datetime.isBlank()) return null;
		try {
			return OffsetDateTime.parse(datetime);
		}catch (DateTimeParseException e) {
			try {
	            return LocalDate.parse(datetime).atStartOfDay().atOffset(ZoneOffset.UTC);
	        } catch (DateTimeParseException e2) {
	            throw new IllegalArgumentException(
	                "Expected ISO 8601 date or datetime, got: " + datetime, e2);
	        }
		}
	}
}
