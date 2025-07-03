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

import java.util.Collection;
import java.util.List;

import org.refractions.cabd.model.assessment.AssessmentType;
import org.refractions.cabd.model.assessment.AssessmentTypeMetadataField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

/**
 * Manage database mapping for Feature types and associated metadata.
 * 
 * @author Emily
 *
 */
@Component
public class AssessmentTypeDao {

	public static final String ASSESSMENT_TYPE_TABLE = "cabd.assessment_types";
	public static final String ASSESSMENT_METADATA_TABLE = "cabd.assessment_type_metadata";

	
	@Autowired
	private JdbcTemplate jdbcTemplate;
		
	/**
	 * Mapper for feature type query to FeatureType 
	 */
	private RowMapper<AssessmentType> typeMapper = (rs, rownum)-> 
		new AssessmentType(rs.getString("type"), 
				rs.getString("data_view"),
				rs.getString("name_en"), 
				rs.getString("name_fr")
				);

	public AssessmentTypeDao() {
	}
	
	/**
	 * Gets all feature types configured in the database.
	 * 
	 * @return
	 */
	public List<AssessmentType> getFeatureTypes(){
		String query = "SELECT type, data_view, name_en, name_fr FROM " + ASSESSMENT_TYPE_TABLE;
		return jdbcTemplate.query(query, typeMapper);		
	}
	
	/**
	 * Gets the metadata for the given type.
	 * @param type
	 * @return
	 */
	public Collection<AssessmentTypeMetadataField> getTypeMetadata(String type) {

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT field_name, ");
		sb.append("name_en, description_en, name_fr, description_fr,data_type");
		sb.append(" FROM ");
		sb.append(ASSESSMENT_METADATA_TABLE);
		sb.append(" WHERE type = ?");
		
		/**
		 * Mapper for metadata field row to FeatureViewMetadataField object
		 */
		RowMapper<AssessmentTypeMetadataField> viewMetadataMapper = (rs, rownum) ->{		
			return new AssessmentTypeMetadataField(
					rs.getString("field_name"), rs.getString("name_en"), 
					rs.getString("description_en"), rs.getString("name_fr"),
					rs.getString("description_fr"), rs.getString("data_type"));
		};

		List<AssessmentTypeMetadataField> fields = jdbcTemplate.query(sb.toString(), 
				viewMetadataMapper, type);
		
		//return the metadata
		return fields;
	}
	
	
}
