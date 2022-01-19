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

import java.util.List;
import java.util.Map;

import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;
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
public class FeatureTypeDao {

	public static final String FEATURE_TYPE_TABLE = "cabd.feature_types";
	public static final String FEATURE_METADATA_TABLE = "cabd.feature_type_metadata";
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	/**
	 * Mapper for feature type query to FeatureType 
	 */
	private RowMapper<FeatureType> typeMapper = (rs, rownum)-> 
		new FeatureType(rs.getString("type"), rs.getString("data_view"), 
				rs.getString("name"), rs.getString("attribute_source_table"));
		
	/**
	 * Mapper for metadata field row to FeatureViewMetadataField object
	 */
	private RowMapper<FeatureViewMetadataField> viewMetadataMapper = (rs, rownum) -> 
		new FeatureViewMetadataField(
				rs.getString("field_name"), rs.getString("name"), 
				rs.getString("description"), rs.getBoolean("is_link"),
				rs.getString("data_type"), (Integer)rs.getObject("vw_simple_order"),
				(Integer)rs.getObject("vw_all_order"), rs.getBoolean("include_vector_tile"));

	
	public FeatureTypeDao() {
	}
	
	/**
	 * Gets all feature types configured in the database.
	 * 
	 * @return
	 */
	public List<FeatureType> getFeatureTypes(){
		String query = "SELECT type, data_view, name, attribute_source_table FROM " + FEATURE_TYPE_TABLE;
		return jdbcTemplate.query(query, typeMapper);		
	}
	
	/**
	 * Gets the metadata for the given view.
	 * @param view
	 * @return
	 */
	public FeatureViewMetadata getViewMetadata(String view) {
		
		String query = "SELECT field_name, name, description, is_link, data_type, vw_simple_order, vw_all_order, include_vector_tile FROM " +
					FEATURE_METADATA_TABLE + " WHERE view_name = ?";
		
		List<FeatureViewMetadataField> fields = jdbcTemplate.query(query, viewMetadataMapper, view);
		
		//get the geometry columns
		String schema = "public";
		String tablename = "";
		String bits[] = view.split("\\.");
		if (bits.length == 2) {
			schema = bits[0];
			tablename = bits[1];
		}else {
			tablename = view;
		}
		
		String geomquery = "SELECT f_geometry_column, srid FROM public.geometry_columns where f_table_schema = ? and f_table_name = ?";
		List<Map<String, Object>> columns = jdbcTemplate.queryForList(geomquery, schema, tablename);
		
		for (FeatureViewMetadataField f : fields) {
			
			for (Map<String,Object> row : columns) {
				String key = (String) row.get("f_geometry_column");
				int srid = (int) row.get("srid");
			
				if (key.equalsIgnoreCase(f.getFieldName())) {
					f.setGeometry(true, srid);
				}
			}
		}
		
		//return the metadata
		return new FeatureViewMetadata(view, fields);
	}
}
