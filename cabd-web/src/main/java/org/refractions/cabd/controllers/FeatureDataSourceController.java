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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.dao.AssessmentDao;
import org.refractions.cabd.dao.FeatureDao;
import org.refractions.cabd.exceptions.ApiError;
import org.refractions.cabd.exceptions.NotFoundException;
import org.refractions.cabd.model.Assessment;
import org.refractions.cabd.model.DataSource;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureSourceDetails;
import org.refractions.cabd.model.FeatureType;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import org.springframework.web.util.UriComponents;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.servlet.http.HttpServletRequest;

/**
 * Controller for feature attribute source details 
 * 
 * @author Emily
 *
 */
@RestController
@RequestMapping("/" + FeatureDataSourceController.PATH)
public class FeatureDataSourceController {
	
	public static final String PATH = FeatureController.PATH + "/datasources";
	
	@Autowired
	private FeatureDao featureDao;
	
	@Autowired
	private AssessmentDao assessmentDao;
	
	@Operation(summary = "Find the feature source attribute details for an individual feature")
	@ApiResponses(value = { 
			@ApiResponse(responseCode = "200",
						description = "CSV text representing the attribute source field data.",
						content = {
						@Content(mediaType = "text/csv")}),
			 @ApiResponse(responseCode = "404",
					 	description = "feature not found", 
			 			content = {
						@Content(mediaType = "application/json", schema = @Schema(implementation = ApiError.class))})})
	@GetMapping(value = "/{id:[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}}",	
			produces = {MediaType.APPLICATION_JSON_VALUE, CabdApplication.CSV_MEDIA_TYPE_STR})

	public ResponseEntity<FeatureSourceDetails> getFeatureSourceDetails(
			@Parameter(description = "unique feature identifier") 
			@PathVariable("id") UUID id,
			@ParameterObject FeatureDataSourceRequestParameters params, 
			HttpServletRequest request) {
		
		FeatureType ftype = featureDao.getFeatureType(id);
		if (ftype == null) {
			throw new NotFoundException(MessageFormat.format("No feature ''{0}'' found.", id.toString()));
		}
		if (ftype.getAttributeSourceTable() == null) {
			throw new NotFoundException(MessageFormat.format("Feature type {0} has no attribute source table configured", ftype.getType()));
		}
		boolean includeall = false;
		if (params.getFields() != null && params.getFields().equalsIgnoreCase("all")) {
			includeall = true;
		}
		
		Feature feature = featureDao.getFeature(id);
		if (feature == null) throw new NotFoundException(MessageFormat.format("No feature with id ''{0}'' found.", id));
		
		FeatureSourceDetails details = new FeatureSourceDetails(id, includeall);
		
		//name
		if (ftype.getDefaultNameField() != null && feature.getAttribute(ftype.getDefaultNameField()) != null) {
			details.setFeatureName(feature.getAttribute(ftype.getDefaultNameField()).toString());
		}
		
		if (ftype.isAssessmentSite()) {
			//we have to do something special for community data assessments and other assessments
			buildAssessmentDataSources(feature.getId(), ftype, details, request);
		} 
		
		//add other data sources (by type)
		List<DataSource> sources = featureDao.getDataSources(id, ftype);
		List<DataSource> dsspatial = new ArrayList<>();
		List<DataSource> dsnon = new ArrayList<>();
		for (DataSource s : sources) {
			if (s.getType().equalsIgnoreCase("spatial")) {
				dsspatial.add(s);
			}else {
				dsnon.add(s);
			}
		}
		details.setSpatialDataSources(dsspatial);
		details.setNonSpatialDataSources(dsnon);
			
		//attribute sources
		List<String[]> fields = featureDao.getFeatureSourceDetails(id, ftype, ftype.isAssessmentSite());
		details.setAttributeDataSources(fields);  //this will merge them if already set
		
		return ResponseEntity.ok(details);		
	}
	
	/*
	 * build datasource details for sites/structures data
	 */
	private void buildAssessmentDataSources(UUID cabdid, FeatureType ftype, FeatureSourceDetails details, HttpServletRequest request) {
		
		UriComponents metadataurl = ServletUriComponentsBuilder.fromContextPath(request)
		        .path("/" + AssessmentController.PATH).build();
		
		List<Assessment> assessments = assessmentDao.getAllAssessments(cabdid);
		assessments.forEach(a->a.setRefUrl(metadataurl.toUriString()));
		
		details.setAssessmentDataSources(assessments);
		
		List<String[]> fields = assessmentDao.getFeatureSourceDetails(cabdid, ftype);
		details.setAttributeDataSources(fields);			
	}
}
