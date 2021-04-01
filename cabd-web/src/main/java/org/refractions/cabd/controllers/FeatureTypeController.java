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
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.exceptions.ApiError;
import org.refractions.cabd.exceptions.NotFoundException;
import org.refractions.cabd.model.FeatureType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import org.springframework.web.util.UriComponents;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

/**
 * Controller for feature types
 * 
 * @author Emily
 *
 */
@RestController
@RequestMapping("/" + FeatureTypeController.PATH)
public class FeatureTypeController {
	
	public static final String PATH = "features/types";
	
	@Autowired
	FeatureTypeManager typeManager;
	
	/**
	 * Gets the feature types supported in the system
	 * @param request
	 * @return
	 */
	@Operation(summary = "Lists all feature types.")
	@ApiResponses(value = { @ApiResponse(responseCode = "200", content = {
			@Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = FeatureType.class))) }) }) 
	@GetMapping(value = "/")
	public ResponseEntity<List<FeatureType>> getFeatureTypes(HttpServletRequest request) {
		
		List<FeatureType> types = typeManager.getFeatureTypes();
		
		//configure urls
		UriComponents dataurl = ServletUriComponentsBuilder.fromContextPath(request)
		        .path("/" + FeatureController.PATH).build();
		UriComponents metadataurl = ServletUriComponentsBuilder.fromContextPath(request)
		        .path("/" + PATH).build();
		types.forEach(e->e.setUrls(dataurl.toUriString(), metadataurl.toUriString()));
		
		return ResponseEntity.ok(types);
	}
	
	/**
	 * Gets the feature schema for given feature
	 * 
	 * @param id
	 * @return
	 */
	@Operation(summary = "Finds the schema for a given feature type")
	@ApiResponses(value = { 
			@ApiResponse(responseCode = "200",
						description = "The feature types schema as Json.",
						content = {
						@Content(mediaType = "application/geo+json")}),
			 @ApiResponse(responseCode = "404",
					 	description = "type not found", 
			 			content = {
						@Content(mediaType = "application/json", schema = @Schema(implementation = ApiError.class))})})
	@GetMapping(value = "/{type:[a-zA-Z0-9_]+}")
	public ResponseEntity<FeatureType> getFeatureSchema(
			@Parameter(description = "feature type") 
			@PathVariable("type") String type,
			HttpServletRequest request) {
		
		type = type.strip().toLowerCase();
		
		FeatureType ftype = typeManager.getFeatureType(type);
		if (ftype == null) throw new NotFoundException(MessageFormat.format("No feature of type ''{0}'' found.", type));

		return ResponseEntity.ok(ftype);

	}
}
