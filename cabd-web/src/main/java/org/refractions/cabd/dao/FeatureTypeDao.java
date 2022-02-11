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
import org.refractions.cabd.model.FeatureTypeListValue;
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
				(Integer)rs.getObject("vw_all_order"), rs.getBoolean("include_vector_tile"), 
				rs.getString("value_options_reference"));

	
	private RowMapper<FeatureTypeListValue> validValueMapper = (rs, rownum) ->
		new FeatureTypeListValue(rs.getObject("value"), rs.getString("name"), rs.getString("description"));
		
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

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT field_name, name, description, is_link, data_type, ");
		sb.append("vw_simple_order, vw_all_order, include_vector_tile, value_options_reference ");
		sb.append(" FROM ");
		sb.append(FEATURE_METADATA_TABLE);
		sb.append(" WHERE view_name = ?");
		
		List<FeatureViewMetadataField> fields = jdbcTemplate.query(sb.toString(), viewMetadataMapper, view);
		
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
		
		sb = new StringBuilder();
		sb.append("SELECT f_geometry_column, srid ");
		sb.append("FROM public.geometry_columns ");
		sb.append("WHERE f_table_schema = ? and f_table_name = ?");
		List<Map<String, Object>> columns = jdbcTemplate.queryForList(sb.toString(), schema, tablename);
		
		for (FeatureViewMetadataField f : fields) {
			
			for (Map<String,Object> row : columns) {
				String key = (String) row.get("f_geometry_column");
				int srid = (int) row.get("srid");
			
				if (key.equalsIgnoreCase(f.getFieldName())) {
					f.setGeometry(true, srid);
				}
			}
		}
		
		//load any list references
		for (FeatureViewMetadataField f : fields) {
			if (f.getValidValuesReference() == null) continue;
			
			bits = f.getValidValuesReference().split(";");
			String listtable = bits[0].trim();
			if (listtable.isBlank()) listtable = null;
			String valuefield = bits[1].trim();
			if (valuefield.isBlank()) valuefield = null;
			String namefield = bits[2].trim();
			if (namefield.isBlank()) namefield = null;
			String descfield = null;
			if (bits.length > 3 && !bits[3].trim().isBlank()) descfield = bits[3].trim();
			
			sb = new StringBuilder();
			sb.append("SELECT ");
			sb.append(valuefield);
			sb.append(" as value, ");
			sb.append(namefield);
			sb.append(" as name, ");
			if (descfield != null) {
				sb.append(descfield);
				sb.append(" as description ");
			}else {
				sb.append("null as description ");
			}
			sb.append(" FROM ");
			sb.append(listtable);
			
			List<FeatureTypeListValue> validValues = 
					jdbcTemplate.query(sb.toString(), validValueMapper);
			
			f.setValueOptions(validValues);
		}
		
		//return the metadata
		return new FeatureViewMetadata(view, fields);
	}
}
