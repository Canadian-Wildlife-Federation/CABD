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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.refractions.cabd.dao.CommunityDataDao;
import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.exceptions.NotFoundException;
import org.refractions.cabd.model.CommunityData;
import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.SimpleFeatureList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;


/**
 * REST api for Community data collection
 * 
 * @author Emily
 *
 */
@RestController
@RequestMapping("/" + CommunityController.PATH)
public class CommunityController {

	public static final String PATH = "community";
	
	@Autowired
	CommunityDataDao communityDao;

	@Autowired
	private CommunityProcessor communityProcessor;
	
	
	@Autowired
	FeatureTypeManager typeManager;
	
	//requires content-type = application/json in request
	@Operation(summary = "Uploads json data from community app.")
	@ApiResponses(value = { 
			@ApiResponse(responseCode = "204")})
	@PostMapping(produces = {MediaType.APPLICATION_JSON_VALUE} )
	public String postData(@RequestBody String featuresJson,
			HttpServletRequest request) {
		
		//save data and return; data is parsed as a part of a separate job
		CommunityData data = new CommunityData(featuresJson, Instant.now());
		saveCommunityData(data);
		communityProcessor.start();
		
		JsonObject result = new JsonObject();
		result.add("id", new JsonPrimitive(data.getId().toString()));
		return result.toString();
	}
	
	/**
	 * Gets the status of a community data upload. If the item has been processed this
	 * will return not found.
	 * 
	 * @param id
	 * @return
	 */
	@Operation(summary = "Gets community data status")
	@GetMapping(value = "/status/{id:[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}}",
			produces = {MediaType.APPLICATION_JSON_VALUE})
	public String getRawCommunityDataStatus(
			@Parameter(description = "unique community data identifier") 
			@PathVariable("id") UUID id,
			HttpServletRequest request) {
		
		CommunityData data = communityDao.getCommunityDataRaw(id);
		if (data == null) throw new NotFoundException(MessageFormat.format("Community data with id {0} not found. Either this item never existed or it has been processed into feature tables without errors.", id));

		JsonObject result = new JsonObject();
		result.add("id", new JsonPrimitive(data.getId().toString()));
		result.add("uploaded_datetime", new JsonPrimitive(data.getUploadeddatetime().atOffset(ZoneOffset.UTC).toString()));
		result.add("status", new JsonPrimitive(data.getStatus().name()));
		result.add("status_message", new JsonPrimitive(data.getStatusMessage()));
		JsonArray warnings = new JsonArray(data.getWarningsArray().length);
		for (String w : data.getWarningsArray()) warnings.add(w);;
		result.add("warnings", warnings);
		
		return result.toString();

	}
	
	/**
	 * Return all ghost features - these are features submitted to the community
	 * data api but not yet reviewed and added to the core cabd tables.
	 * 
	 * @return list of ghost features with id and type
	 * 
	 */
	@Operation(summary = "Gets community data ghost features")
	@GetMapping(value = "/ghost",
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/geo+json"})
	public ResponseEntity<SimpleFeatureList> getAllGhostFeatures(HttpServletRequest request) {
		return getAllGhostFeatures(request, null);
	}
	
	/**
	 * Return all ghost features for a given feature type
	 * @param request
	 * @param type
	 * @return
	 */
	@Operation(summary = "Gets community data ghost features of specific type")
	@GetMapping(value = "/ghost/{type:[a-zA-Z0-9_]+}",
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/geo+json"})
	public ResponseEntity<SimpleFeatureList> getAllGhostFeatures(HttpServletRequest request,
			@PathVariable("type") String type) {
		
		List<FeatureType> types = new ArrayList<>();
		if (type == null || type.isBlank()) {
			types.addAll(typeManager.getFeatureTypes());
		}else {
			FeatureType ftype = typeManager.getFeatureType(type);
			if (ftype == null) {
				throw new NotFoundException(MessageFormat.format("The feature type {0} does not exist.", type));
			}
			types.add(ftype);
		}
		return ResponseEntity.ok(communityDao.getGhostFeatures(types));
		
	}
	
	@Transactional
	private void saveCommunityData(CommunityData cd) {
		communityDao.saveRawData(cd);
	}
	
}
