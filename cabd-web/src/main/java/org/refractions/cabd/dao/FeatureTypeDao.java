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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.FeatureTypeListValue;
import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;
import org.refractions.cabd.model.FeatureViewMetadataFieldData;
import org.refractions.cabd.model.FeatureViewMetadataFieldData.MetadataValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
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
				rs.getString("data_version"),
				rs.getString("name_en"), rs.getString("name_fr"), 
				rs.getString("attribute_source_table"),
				rs.getString("feature_source_table"),
				rs.getString("default_featurename_field"), rs.getString("description"));
		
	/**
	 * Mapper for metadata field row to FeatureViewMetadataField object
	 */
	private RowMapper<FeatureViewMetadataField> viewMetadataMapper = (rs, rownum) -> 
		new FeatureViewMetadataField(
				rs.getString("field_name"), rs.getString("name_en"), 
				rs.getString("description_en"), rs.getString("name_fr"),
				rs.getString("description_fr"), rs.getBoolean("is_link"),
				rs.getString("data_type"), (Integer)rs.getObject("vw_simple_order"),
				(Integer)rs.getObject("vw_all_order"), rs.getBoolean("include_vector_tile"), 
				rs.getString("value_options_reference"), rs.getBoolean("is_name_search"), rs.getString("shape_field_name"));

	
	private RowMapper<FeatureTypeListValue> validValueMapper = (rs, rownum) ->
		new FeatureTypeListValue(rs.getObject("value"), rs.getString("name_en"),
				rs.getString("name_fr"),rs.getString("description_en"), rs.getString("description_fr"));
		
	private RowMapper<FeatureTypeListValue> validBboxValueMapper = (rs, rownum) ->
		new FeatureTypeListValue(rs.getObject("value"), rs.getString("name_en"),
				rs.getString("name_fr"),rs.getString("description_en"), rs.getString("description_fr"),
				rs.getDouble("minx"), rs.getDouble("miny"), rs.getDouble("maxx"), rs.getDouble("maxy"));
		
	public FeatureTypeDao() {
	}
	
	
	/**
	 * Gets all feature types configured in the database.
	 * 
	 * @return
	 */
	public List<FeatureType> getFeatureTypes(){
		String query = "SELECT type, data_view, data_version, name_en, name_fr, attribute_source_table, feature_source_table, default_featurename_field, description FROM " + FEATURE_TYPE_TABLE;
		return jdbcTemplate.query(query, typeMapper);		
	}
	
	/**
	 * Gets the metadata for the given view.
	 * @param view
	 * @return
	 */
	public FeatureViewMetadata getViewMetadata(String view) {

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT field_name, ");
		sb.append("name_en, description_en, name_fr, description_fr,");
		sb.append("is_link, data_type, ");
		sb.append("vw_simple_order, vw_all_order, include_vector_tile, value_options_reference, ");
		sb.append("is_name_search, shape_field_name");
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
		//assumption here is that there is an _en table for the view
		List<Map<String, Object>> columns = jdbcTemplate.queryForList(sb.toString(), schema, tablename + "_en");
		
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
			String geomfield = null;
			if (bits.length > 4 && !bits[4].trim().isBlank()) geomfield = bits[4].trim();
			
			sb = new StringBuilder();
			sb.append("SELECT ");
			if (valuefield != null) {
				sb.append(valuefield + " as value, ");
			}else {
				sb.append("null as value, ");
			}
			sb.append(namefield + "_en as name_en, ");
			sb.append(namefield + "_fr as name_fr, ");
			if (descfield != null) {
				sb.append(descfield + "_en as description_en, ");
				sb.append(descfield + "_fr as description_fr, ");
			}else {
				sb.append("null as description_en, ");
				sb.append("null as description_fr, ");
			}
			if (geomfield != null) {
				sb.append("st_xmin(box2d(st_transform(" + geomfield + ", 4617))) as minx, ");
				sb.append("st_ymin(box2d(st_transform(" + geomfield + ", 4617))) as miny, ");
				sb.append("st_xmax(box2d(st_transform(" + geomfield + ", 4617))) as maxx, ");
				sb.append("st_ymax(box2d(st_transform(" + geomfield + ", 4617))) as maxy ");
			}else {
				sb.append("null as minx, ");
				sb.append("null as miny, ");
				sb.append("null as maxx, ");
				sb.append("null as maxy ");
				
			}
			sb.append(" FROM ");
			sb.append(listtable);
			
			RowMapper<FeatureTypeListValue> mapper = validValueMapper;
			if (geomfield != null) {
				mapper = validBboxValueMapper;
			}
			List<FeatureTypeListValue> validValues = 
					jdbcTemplate.query(sb.toString(), mapper);
			
			f.setValueOptions(validValues);
		}
		
		//return the metadata
		return new FeatureViewMetadata(view, fields);
	}
	
	
	/**
	 * Compute the data metadata associated with a feature type (min, max values etc). This is not
	 * static and will change as the data changes
	 * 
	 * @param type feature type to compute data metadata for
	 * @return
	 */
	public Map<FeatureViewMetadataField, FeatureViewMetadataFieldData> computeDataMetadata(FeatureType type) {
		
		//compute min/max values for numeric and date fields
		List<FeatureViewMetadataField> dataFields = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		for (FeatureViewMetadataField field : type.getViewMetadata().getFields()) {
			if (field.getValidValuesReference() != null) continue;

			Class<?> datatype =field.getDataTypeAsClass();
			if (datatype.equals(Double.class) || datatype.equals(Integer.class) || datatype.equals(Date.class)) {
				//compute min/max value
				sb.append("min(" + field.getFieldName() + "), max("+field.getFieldName() + "), ");
				dataFields.add(field);
			}
		}
		if (dataFields.isEmpty()) return Collections.emptyMap();
		sb.delete(sb.length()-2, sb.length());
		sb.append(" FROM ");
		sb.append(type.getDataView());
		
		Map<FeatureViewMetadataField, FeatureViewMetadataFieldData> data = jdbcTemplate.query(sb.toString(), new ResultSetExtractor<Map<FeatureViewMetadataField, FeatureViewMetadataFieldData>>() {

			@Override
			public Map<FeatureViewMetadataField, FeatureViewMetadataFieldData> extractData(ResultSet rs) throws SQLException, DataAccessException {
				if (!rs.next()) return Collections.emptyMap();
				Map<FeatureViewMetadataField, FeatureViewMetadataFieldData> map = new HashMap<>();
				for (int i = 0; i < dataFields.size(); i ++) {
					Object min = rs.getObject(i*2+1);
					Object max = rs.getObject(i*2+2);
					FeatureViewMetadataFieldData data = new FeatureViewMetadataFieldData(dataFields.get(i));
					data.addData(MetadataValue.MIN, min);
					data.addData(MetadataValue.MAX, max);
					map.put(data.getField(),data);	
				}
				return map;
			}});
		return data;
		
	}
}
