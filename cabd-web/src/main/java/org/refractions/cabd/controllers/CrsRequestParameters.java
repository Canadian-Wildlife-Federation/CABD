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
import java.text.MessageFormat;

import org.refractions.cabd.exceptions.InvalidParameterException;
import org.refractions.cabd.model.CrsInfo;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import io.swagger.v3.oas.annotations.Parameter;

/**
 * Class to represent the crs query parameter.  This is the only
 * parameter supported by requests that return a single, already
 * identified, feature; other request parameter classes extend this
 * one to add their own parameters.
 *
 * @author Emily
 *
 */
public class CrsRequestParameters {

	@Parameter(name="crs", required = false, description = "The CRS to return the results in in the form <Authority>:<Code> (ex. EPSG:4326), but default all values are return in EPSG:4617")
	private String crs;

	@ConstructorProperties({"crs"})
	public CrsRequestParameters(String crs) {
		this.crs = crs;
	}

	public String getCrs() { return this.crs; }

	/**
	 * Validates the crs parameter and returns the database srid
	 * associated with it.
	 *
	 * @param jdbcTemplate
	 * @return the database srid, or null if no crs parameter was provided
	 */
	public CrsInfo validateAndGetSrid(JdbcTemplate jdbcTemplate) {
		return parseAndValidateCrsParameter(this.crs, jdbcTemplate);
	}

	/**
	 * Parses a crs parameter of the form <Authority>:<Code> and validates
	 * that the coordinate reference system is supported by the database.
	 *
	 * @param crs
	 * @param jdbcTemplate
	 * @return the database srid, or null if no crs parameter was provided
	 */
	public static CrsInfo parseAndValidateCrsParameter(String crs, JdbcTemplate jdbcTemplate) {
		if (crs == null || crs.trim().isBlank()) return null;

		String[] bits = crs.split(":");
		if (bits.length != 2) {
			throw new InvalidParameterException("This crs parameter is invalid. It must be of the form <Authority>:<Code>");
		}
		Integer code = null;
		try {
			code = Integer.parseInt(bits[1].trim());
		}catch(Exception ex) {
			throw new InvalidParameterException("This crs parameter is invalid. It must be of the form <Authority>:<Code>; where code is a valid integer");
		}

		// validate that the crs exists
		String sql = "SELECT srid FROM public.spatial_ref_sys WHERE auth_name = ? AND auth_srid = ?";
		try {
			Integer pgSrid = jdbcTemplate.queryForObject(sql, Integer.class, bits[0].trim().toUpperCase(), code);
			return new CrsInfo(bits[0].trim().toUpperCase(), code, pgSrid);
		} catch (EmptyResultDataAccessException e) {
			throw new InvalidParameterException(MessageFormat.format("The crs {0} is not supported.", crs));
		}
	}
}
