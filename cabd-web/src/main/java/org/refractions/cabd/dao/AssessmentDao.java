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
package org.refractions.cabd.dao;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.refractions.cabd.CabdConfigurationProperties;
import org.refractions.cabd.controllers.AttributeSet;
import org.refractions.cabd.controllers.ParsedRequestParameters;
import org.refractions.cabd.controllers.TooManyFeaturesException;
import org.refractions.cabd.controllers.VectorTileController;
import org.refractions.cabd.exceptions.InvalidDatabaseConfigException;
import org.refractions.cabd.exceptions.NotFoundException;
import org.refractions.cabd.model.DataSource;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureList;
import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;
import org.refractions.cabd.model.assessment.AssessmentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Manager features 
 * 
 * @author Emily
 *
 */
@Component
public class AssessmentDao {

	/**
	 * SRID of geometry in database
	 */
	public static int DATABASE_SRID = 4617;
	
	/**
	 * ID Field for features
	 */
	public static final String ID_FIELD = "assessment_id";

    
	@Autowired
	private JdbcTemplate jdbcTemplate;
	@Autowired
	private ObjectMapper objectMapper;

	
	@Autowired
	private AssessmentTypeManager typeManager;
	
	@Autowired
	CabdConfigurationProperties properties;
	
	/**
	 * Finds the feature with the given uuid.  Will return null
	 * if no feature is found.
	 * 
	 * @param uuid
	 * @return
	 * @throws JsonProcessingException 
	 * @throws JsonMappingException 
	 */
	public JsonNode getAssessment(UUID assessment_id) throws JsonMappingException, JsonProcessingException {
		
		String assessmentJson = null;
		for (AssessmentType t : typeManager.getAssessmentTypes()) {
			String query = "SELECT to_jsonb(b) FROM " + t.getDataView() + " b WHERE b.assessment_id = ? ";
			try {
				assessmentJson = jdbcTemplate.queryForObject(query, String.class, assessment_id);
			}catch (EmptyResultDataAccessException ex) {
				//eat this exception
			}
			if (assessmentJson != null) break;
		}
		if (assessmentJson == null) {
			throw new NotFoundException(MessageFormat.format("No assessment with id {0} found.", assessment_id));
		}
		return objectMapper.readTree(assessmentJson);
	}
	
	//get all assessments for a cabd order by date
	public JsonNode getAssessments(UUID cabd_id) throws JsonMappingException, JsonProcessingException {
		
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT jsonb_agg(assessment_json) FROM (");
		sb.append("SELECT assessment_json FROM (");
		
		StringJoiner joiner = new StringJoiner(" UNION ");
		for (AssessmentType t : typeManager.getAssessmentTypes()) {
			joiner.add("SELECT to_jsonb(b) as assessment_json, assessment_date FROM " + t.getDataView() + " b WHERE b.cabd_id = ? ");			
		}
		sb.append(joiner.toString());
		sb.append(" ) ORDER BY assessment_date desc ) ");
		
		try {
			String json = jdbcTemplate.queryForObject(sb.toString(), String.class, ((List<UUID>)Collections.nCopies(typeManager.getAssessmentTypes().size(), cabd_id)).toArray());
			return objectMapper.readTree(json);
		}catch (EmptyResultDataAccessException ex) {
			//eat this exception
		}
		return objectMapper.createArrayNode();
		
	}
	


}
