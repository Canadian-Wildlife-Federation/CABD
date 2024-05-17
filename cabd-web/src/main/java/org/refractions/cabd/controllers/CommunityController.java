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

import java.time.Instant;

import javax.servlet.http.HttpServletRequest;

import org.refractions.cabd.dao.CommunityDataDao;
import org.refractions.cabd.model.CommunityData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
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
	
	//requires content-type = application/json in request
	@Operation(summary = "Uploads json data from community app.")
	@ApiResponses(value = { 
			@ApiResponse(responseCode = "204")})

	@PostMapping
	public ResponseEntity<Object> postData(@RequestBody String featuresJson,
			HttpServletRequest request) {
		
		//save data and return; data is parsed as a part of a separate job
		saveCommunityData(new CommunityData(featuresJson, Instant.now()));
		communityProcessor.start();
		System.out.println("next");
		return ResponseEntity.status(HttpStatus.OK).build();
	}
	
	
	
	@Transactional
	private void saveCommunityData(CommunityData cd) {
		communityDao.saveRawData(cd);
	}
	
}
