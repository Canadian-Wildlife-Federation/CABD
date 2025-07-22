/*
 * Copyright 2025 Canadian Wildlife Federation
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
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.refractions.cabd.CabdConfigurationProperties;
import org.refractions.cabd.dao.AssessmentDao;
import org.refractions.cabd.dao.AssessmentTypeManager;
import org.refractions.cabd.exceptions.NotFoundException;
import org.refractions.cabd.model.assessment.AssessmentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import org.springframework.web.util.UriComponents;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;

/**
 * REST api for assessment data
 * 
 * @author Emily
 *
 */
@RestController
@RequestMapping("/" + AssessmentController.PATH)
public class AssessmentController {

	public static final String PATH = "assessments";
	
	public static final int MAX_RESULTS = 1000;
	
	
	@Autowired
	CabdConfigurationProperties properties;
	
	@Autowired
	AssessmentDao assessmentDao;
	
	@Autowired
	AssessmentTypeManager typeManager;
		
	/**
	 * Gets an individual assessment by identifier. Returns are only returned as json.
	 * 
	 * @param id
	 * @return
	 */
	@Operation(summary = "Gets a feature by id.")
	@GetMapping(value = "/{id:[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}}",
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/json"})
	public ResponseEntity<JsonNode> getAssessment(
			@Parameter(description = "unique feature identifier") 
			@PathVariable("id") UUID id,
			HttpServletRequest request) {
		
		JsonNode f;
		try {
			f = assessmentDao.getAssessment(id);
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage(),ex);
		}		
		return ResponseEntity.ok(f);
	}
	
	/**
	 * Gets all assessments as JSON array for a given cabd feature
	 * 
	 * @param id
	 * @return
	 */
	@Operation(summary = "Gets a feature by type and id.")
	@GetMapping(value = "/cabd/{cabdid:[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}}",
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/json"})
	public ResponseEntity<JsonNode> getAssessmentsByCabdId(
			@Parameter(description = "unique feature identifier") 
			@PathVariable("cabdid") UUID cabdid,
			HttpServletRequest request) {
		
		JsonNode f;
		try {
			f = assessmentDao.getAssessments(cabdid);
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage(),ex);
		}
		return ResponseEntity.ok(f);
	}
	
	/**
	 * Gets list of all assessment types 
	 * 
	 * @return
	 */
	@Operation(summary = "Gets the assessment types.")
	@GetMapping(value = "/types/",
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/json"})
	public String getTypes(HttpServletRequest request) {	
		
		List<AssessmentType> types = typeManager.getAssessmentTypes();
		UriComponents metadataurl = ServletUriComponentsBuilder.fromContextPath(request)
		        .path("/" + PATH).build();
		types.forEach(e->e.setUrls(metadataurl.toUriString() + "/types"));
		
		ObjectMapper mapper = new ObjectMapper();
		ArrayNode jsontypes = mapper.createArrayNode();
		for (AssessmentType type: types ) {
			ObjectNode jsontype = mapper.createObjectNode();
			jsontype.put("type", type.getType());
			jsontype.put("name", type.getName());
			jsontype.put("metadata", type.getMetadataUrl());
			jsontypes.add(jsontype);			
		}
		
		try {
			return mapper.writeValueAsString(jsontypes);
		} catch (JsonProcessingException ex) {
			throw new RuntimeException(ex);
		}
	}
	
	/**
	 * Gets metadata details about a given assessment type
	 * 
	 * @param type assessment type
	 * @return
	 */
	@Operation(summary = "Gets the assessment type metadata")
	@GetMapping(value = "/types/{type:[a-zA-Z0-9_-]+}",
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/json"})
	public AssessmentType getTypeMetadata(
			@Parameter(description = "the assessment type to get metadata for") 
			@PathVariable("type") String type,
			HttpServletRequest request) {	
		
		AssessmentType atype = typeManager.getAssessmentType(type);
		if (atype == null) {
			throw new NotFoundException(MessageFormat.format("The assessment type {0} does not exist.", type));
		}
		return atype;
	}
			
}
