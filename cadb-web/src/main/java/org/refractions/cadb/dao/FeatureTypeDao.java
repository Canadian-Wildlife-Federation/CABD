package org.refractions.cadb.dao;

import java.util.List;
import java.util.Map;

import org.refractions.cadb.model.FeatureType;
import org.refractions.cadb.model.FeatureViewMetadata;
import org.refractions.cadb.model.FeatureViewMetadataField;
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

	public static final String FEATURE_TYPE_TABLE = "cadb.feature_types";
	public static final String FEATURE_METADATA_TABLE = "cadb.feature_type_metadata";
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	/**
	 * Mapper for feature type query to FeatureType 
	 */
	private RowMapper<FeatureType> typeMapper = (rs, rownum)-> 
		new FeatureType(rs.getString("type"), rs.getString("data_view"));
		
	/**
	 * Mapper for metadata field row to FeatureViewMetadataField object
	 */
	private RowMapper<FeatureViewMetadataField> viewMetadataMapper = (rs, rownum) -> 
		new FeatureViewMetadataField(
				rs.getString("field_name"), rs.getString("name"), 
				rs.getString("description"), rs.getBoolean("is_link"));

	
	public FeatureTypeDao() {
	}
	
	/**
	 * Gets all feature types configured in the database.
	 * 
	 * @return
	 */
	public List<FeatureType> getFeatureTypes(){
		String query = "SELECT type, data_view FROM " + FEATURE_TYPE_TABLE;
		return jdbcTemplate.query(query, typeMapper);		
	}
	
	/**
	 * Gets the metadata for the given view.
	 * @param view
	 * @return
	 */
	public FeatureViewMetadata getViewMetadata(String view) {
		
		String query = "SELECT field_name, name, description, is_link FROM " +
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
