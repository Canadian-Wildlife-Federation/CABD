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
package org.refractions.cabd.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;

import org.refractions.cabd.CabdConfigurationProperties;
import org.refractions.cabd.exceptions.NotFoundException;
import org.refractions.cabd.model.Assessment;
import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.assessment.AssessmentType;
import org.refractions.cabd.model.assessment.AssessmentType.RawAssessmentType;
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

/**
 * Manager for assessment data 
 * 
 * @author Emily
 *
 */
@Component
public class AssessmentDao {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private ObjectMapper objectMapper;
	
	@Autowired
	private AssessmentTypeManager typeManager;
	
	@Autowired
	private FeatureTypeManager ftypeManager;
	
	@Autowired
	CabdConfigurationProperties properties;
	
	/**
	 * Finds the assessment with the given uuid.  Will throw exception if not found.
	 * Data is returned as json.
	 * 
	 * @param uuid assessment id
	 * 
	 * @return
	 * 
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
	
	/**
	 * Finds all assessments for a given cabd_id orderd by date. Data is returned
	 * as a jsonarray
	 * 
	 * @param cabd_id cabd feature id
	 * 
	 * @return
	 * 
	 * @throws JsonProcessingException 
	 * @throws JsonMappingException 
	 */
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
	
	/**
	 * Gets all assessments for a given feature. This only returns basic
	 * assessment details.
	 * 
	 * @param cabd_id
	 * @return
	 */
	public List<Assessment> getAllAssessments(UUID cabd_id){
		StringJoiner joiner = new StringJoiner(" UNION ");
		for (AssessmentType t : typeManager.getAssessmentTypes()) {
			joiner.add("SELECT assessment_id, assessment_date, '" + t.getType() + "' as assessment_type FROM " + t.getDataView() + " b WHERE b.cabd_id = ? ");			
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT assessment_id, assessment_date, assessment_type FROM (");
		sb.append(joiner.toString());
		sb.append(" ) ORDER BY assessment_date desc  ");
		
		
		List<Assessment> assessments = jdbcTemplate.query(sb.toString(),
				new RowMapper<Assessment>() {
			@Override
			public Assessment mapRow(ResultSet rs, int rowNum) throws SQLException {
				UUID uuid = (UUID)rs.getObject(1);
				LocalDateTime d = rs.getTimestamp(2).toLocalDateTime();
				String type = rs.getString(3);
				return new Assessment(uuid, d, type);
			}},
			((List<UUID>)Collections.nCopies(typeManager.getAssessmentTypes().size(), cabd_id)).toArray()
		);
		
		return assessments;
	}

	
	/**
	 * Finds all the assessment source details for a given feature. Returns 
	 * a string array where the first element is the field name, the second
	 * the assessment identifier, the third the assessment type (if applicable)
	 * Values are sorted by attribute name.
	 * 
	 * Includes all structures with field names prefixed by structure number
	 * 
	 * @param featureId the site_id
	 * @param ftype the cabd feature type
	 * @return
	 */
	public List<String[]> getFeatureSourceDetails(UUID featureId, FeatureType ftype) {
		
		if (!ftype.isAssessmentSite()) {
			throw new RuntimeException("Assessment feature source details are only available for feature types defined as assessment feature types");
		}
		
		//for the main assessment data
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT substring(column_name, 0, length(column_name) - length('_dsid') + 1)");
		sb.append(" FROM information_schema.columns ");
		sb.append("WHERE table_schema = ? and table_name = ? and column_name like '%_dsid'");
		String[] parts = ftype.getAttributeSourceTable().split("\\.");
		
		String sname = parts[0];
		String tname = parts[1];
		
		List<String> columns = jdbcTemplate.query(sb.toString(), 
				new Object[] {sname, tname}, 
				new int[] {Types.VARCHAR, Types.VARCHAR}, new RowMapper<String>() {
			@Override
			public String mapRow(ResultSet rs, int rowNum) throws SQLException {
				return rs.getString(1);
			}});
		
		sb = new StringBuilder();
		sb.append("SELECT ");
		for (String field : columns) {
			sb.append(field + "_src,");
			sb.append(field + "_dsid,");
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(" FROM ");
		sb.append(ftype.getAttributeSourceTable());
		sb.append(" WHERE cabd_id = ?");
	
		List<List<String[]>> fieldData = jdbcTemplate.query(sb.toString(),
				new Object[] {featureId}, new int[] {SqlTypeValue.TYPE_UNKNOWN}, 
				new RowMapper<List<String[]>>() {
			
			@Override
			public List<String[]> mapRow(ResultSet rs, int rowNum) throws SQLException {
				List<String[]> columnData = new ArrayList<>();
				
				for (String field: columns) {
					String srctype = rs.getString(field + "_src");
					UUID dsuuid = (UUID) rs.getObject(field + "_dsid");
					
					if (srctype == null) {
						columnData.add(new String[] {field, "", ""});
						continue;
					}
					
					AssessmentType.RawAssessmentType type = AssessmentType.RawAssessmentType.findType(srctype);
					if (type == RawAssessmentType.MODELLED_CROSSINGS || type == RawAssessmentType.SATELLITE) {
						columnData.add(new String[] {field, type.getDataSourceName(), ""});					
					}else {
						//community or assessment
						if (dsuuid != null) {
							columnData.add(new String[] {field, dsuuid.toString(), type.getDataSourceName()});
						}else {
							columnData.add(new String[] {field, "unknown", type.getDataSourceName()});
						}					
					}
				}
				return columnData;
		}});
		
		List<String[]> attributesources = new ArrayList<>();
		if (!fieldData.isEmpty()) {
			attributesources = fieldData.get(0);
		}
		
		attributesources.sort((a,b)->a[0].compareTo(b[0]));
		
		
		//add structures data sources
		FeatureType stype = ftypeManager.getFeatureType(FeatureTypeManager.STRUCTURE_FEATURE_TYPE);
		
		//TODO: we may not want to hard code this table name
		//it is in the metadata table but not currently loaded into memory
		List<Object[]> structures = jdbcTemplate.query(
				"SELECT structure_id, structure_number FROM stream_crossings.structures WHERE site_id = ? order by structure_number ",
				(rs, rowNum) -> new Object[] {(UUID) rs.getObject(1), rs.getInt(2)},
				featureId);
				
		sb = new StringBuilder();
		sb.append("SELECT substring(column_name, 0, length(column_name) - length('_dsid') + 1)");
		sb.append(" FROM information_schema.columns ");
		sb.append("WHERE table_schema = ? and table_name = ? and column_name like '%_dsid'");
		
		parts = stype.getAttributeSourceTable().split("\\.");
		sname = parts[0];
		tname = parts[1];
		
		List<String> scolumns = jdbcTemplate.query(sb.toString(), 
				new Object[] {sname, tname}, 
				new int[] {Types.VARCHAR, Types.VARCHAR}, new RowMapper<String>() {
			@Override
			public String mapRow(ResultSet rs, int rowNum) throws SQLException {
				return rs.getString(1);
			}});
		
		for (Object[] structure : structures) {
			UUID uuid = (UUID) structure[0];
			int num = (int) structure [1];
			
			sb = new StringBuilder();
			sb.append("SELECT ");
			for (String field : scolumns) {
				sb.append(field + "_src,");
				sb.append(field + "_dsid,");
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append(" FROM ");
			sb.append(stype.getAttributeSourceTable());
			sb.append(" WHERE structure_id = ?");
		
			fieldData = jdbcTemplate.query(sb.toString(),
					new Object[] {uuid}, new int[] {SqlTypeValue.TYPE_UNKNOWN}, 
					new RowMapper<List<String[]>>() {
				
				@Override
				public List<String[]> mapRow(ResultSet rs, int rowNum) throws SQLException {
					List<String[]> columnData = new ArrayList<>();
					
					for (String field: scolumns) {
						String srctype = rs.getString(field + "_src");
						UUID dsuuid = (UUID) rs.getObject(field + "_dsid");
						
						String fieldkey = num + "_" + field;
						
						if (srctype == null) {
							columnData.add(new String[] {fieldkey, "", ""});
							continue;
						}
						
						AssessmentType.RawAssessmentType type = AssessmentType.RawAssessmentType.findType(srctype);
						if (type == RawAssessmentType.MODELLED_CROSSINGS || type == RawAssessmentType.SATELLITE) {
							columnData.add(new String[] {field, type.getDataSourceName(), ""});					
						}else {
							//community or assessment
							if (dsuuid != null) {
								columnData.add(new String[] {field, dsuuid.toString(), type.getDataSourceName()});
							}else {
								columnData.add(new String[] {field, "unknown", type.getDataSourceName()});
							}					
						}
					}
					return columnData;
			}});
			if (fieldData.get(0) != null) {
				fieldData.get(0).sort((a,b)->a[0].compareTo(b[0]));
				attributesources.addAll(fieldData.get(0));
			}
		}
				
		
		return attributesources;
	}

}
