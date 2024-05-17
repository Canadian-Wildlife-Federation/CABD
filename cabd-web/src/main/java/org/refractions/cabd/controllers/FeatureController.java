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

import java.text.MessageFormat;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.CabdConfigurationProperties;
import org.refractions.cabd.dao.FeatureDao;
import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.dao.UserFeatureUpdateDao;
import org.refractions.cabd.exceptions.ApiError;
import org.refractions.cabd.exceptions.InvalidParameterException;
import org.refractions.cabd.exceptions.NotFoundException;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureChangeRequest;
import org.refractions.cabd.model.FeatureList;
import org.refractions.cabd.model.FeatureType;
import org.springdoc.api.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

/**
 * REST api for database features
 * 
 * @author Emily
 *
 */
@RestController
@RequestMapping("/" + FeatureController.PATH)
public class FeatureController {

	public static final String PATH = "features";
	
	public static final int MAX_RESULTS = 1000;
	
	@Autowired
	CabdConfigurationProperties properties;
	
	@Autowired
	FeatureDao featureDao;
	@Autowired
	UserFeatureUpdateDao featureUpdateDao;
	@Autowired
	FeatureTypeManager typeManager;
		
	/**
	 * Gets an individual feature by identifier. Search the all_features
	 * view for the feature type.
	 * 
	 * @param id
	 * @return
	 */
	@Operation(summary = "Gets a feature by id.")
	@GetMapping(value = "/{id:[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}}",
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/geo+json",
					CabdApplication.CSV_MEDIA_TYPE_STR,
					CabdApplication.GEOPKG_MEDIA_TYPE_STR})
	public ResponseEntity<Feature> getFeature(
			@Parameter(description = "unique feature identifier") 
			@PathVariable("id") UUID id,
			HttpServletRequest request) {
		
		Feature f = featureDao.getFeature(id);
		if (f == null) throw new NotFoundException(MessageFormat.format("No feature with id ''{0}'' found.", id));
		return ResponseEntity.ok(f);
	}
	
	/**
	 * Gets an individual feature by type and identifier
	 * 
	 * @param id
	 * @return
	 */
	@Operation(summary = "Gets a feature by type and id.")
	@GetMapping(value = "/{type:[a-zA-Z0-9_]+}/{id:[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}}",
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/geo+json",
					CabdApplication.CSV_MEDIA_TYPE_STR,
					CabdApplication.GEOPKG_MEDIA_TYPE_STR})
	public ResponseEntity<Feature> getFeatureByTypeAndId(
			@Parameter(description = "unique feature identifier") 
			@PathVariable("type") String type,
			@PathVariable("id") UUID id,
			HttpServletRequest request) {
		
		Feature f = featureDao.getFeature(type, id);
		if (f == null) throw new NotFoundException(MessageFormat.format("No feature with id ''{0}'' found.", id));
		return ResponseEntity.ok(f);
	}
	
	//requires content-type = application/json in request
	@Operation(summary = "Stores feature updates suggested by UI users.")
	@ApiResponses(value = { 
			@ApiResponse(responseCode = "200")})						
	@PutMapping(value = "/{id:[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}}")
	public ResponseEntity<Object> putFeature(
			@Parameter(description = "unique feature identifier") 
			@PathVariable("id") UUID id,
			@RequestBody FeatureChangeRequest changeRequest,
			HttpServletRequest request) {
		
		String error = changeRequest.validate();
		if (error != null) throw new InvalidParameterException(error);
		
		featureUpdateDao.newFeatureUpdate(id, changeRequest);
		
		return ResponseEntity.ok().build();
	}
	
	/**
	 * Query features of a specific type by location, or distance to point.
	 * 
	 * The feature features returned contain attribute all
	 * attributes defined for the provided feature type.
	 * 
	 * @param type
	 * @param parameters
	 * @return
	 */
	//TODO: document that feature types can only contain a-Z0-9_ characters
	@Operation(summary = "Searches for features of a given type.")
	@ApiResponses(value = { 
			@ApiResponse(responseCode = "200",
						description = "Return all feature features that match search parameters as a GeoJson feature collection. Feature will contain all attributes associated with the given type.",
						content = {
						@Content(mediaType = "application/geo+json")}),
			@ApiResponse(responseCode = "400",
					 	description = "When one of the parameters is not in a valid format", 
			 			content = {
						@Content(mediaType = "application/json", schema = @Schema(implementation = ApiError.class))}), 
			@ApiResponse(responseCode = "404",
					 	description = "Feature type not found", 
			 			content = {
						@Content(mediaType = "application/json", schema = @Schema(implementation = ApiError.class))}),			 
			 })
	@GetMapping(value = "/{type:[a-zA-Z0-9_]+}",
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/geo+json", 
					CabdApplication.GEOPKG_MEDIA_TYPE_STR,
					CabdApplication.SHP_MEDIA_TYPE_STR, 
					CabdApplication.KML_MEDIA_TYPE_STR,
					CabdApplication.CSV_MEDIA_TYPE_STR})
	public ResponseEntity<FeatureList> getFeatureByType(
			@Parameter(description = "the feature type to search") @PathVariable("type") String type,
			@ParameterObject FeatureRequestParameters params, HttpServletRequest request) {
		
		ParsedRequestParameters parameters = params.parseAndValidate(typeManager);

		FeatureType btype = typeManager.getFeatureType(type);
		if (btype == null) throw new NotFoundException(MessageFormat.format("The feature type ''{0}'' is not supported.", type));
		return ResponseEntity.ok(featureDao.getFeatures(btype, parameters));
	}
	
	/**
	 * Query features across all types by type, location, or distance to point.
	 * 
	 * The feature features returned contain only attributes
	 * shared across all feature types.
	 * 
	 * @param parameters
	 * @return
	 */
	@Operation(summary = "Searches for features of any type.")
	@ApiResponses(value = { 			
			@ApiResponse(responseCode = "200",
						description = "Return all feature features that match search parameters as a GeoJson feature collection. Feature will contain a limited set of shared attributes associated with all attribute types.",
						content = {
						@Content(mediaType = "application/geo+json")}),
			 @ApiResponse(responseCode = "400",
					 	description = "If one of the feature types is not found or the parameters are not in the expected format. ", 
			 			content = {
						@Content(mediaType = "application/json", schema = @Schema(implementation = ApiError.class))})})
	@GetMapping(value = "",
		produces = {MediaType.APPLICATION_JSON_VALUE, "application/geo+json", 
				CabdApplication.GEOPKG_MEDIA_TYPE_STR,
				CabdApplication.SHP_MEDIA_TYPE_STR, 
				CabdApplication.KML_MEDIA_TYPE_STR,
				CabdApplication.CSV_MEDIA_TYPE_STR})
	
	public ResponseEntity<FeatureList> getFeatures(
			@ParameterObject FeatureRequestTypeParameters params, 
			HttpServletRequest request) {

		ParsedRequestParameters parameters = params.parseAndValidate(typeManager);
		return ResponseEntity.ok(featureDao.getFeatures(parameters.getFeatureTypes(), parameters));
	}
		
}
