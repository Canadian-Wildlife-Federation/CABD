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

import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.refractions.cabd.exceptions.InvalidDatabaseConfigException;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Manager features 
 * 
 * @author Emily
 *
 */
@Component
public class FeatureDao {

	/**
	 * SRID of geometry in database
	 */
	public static int DATABASE_SRID = 4326;
	/**
	 * Valid bounds for SRID
	 */
	public static Envelope VALID_BOUNDS = new Envelope(-180,180,-90, 90);
	/**
	 * ID Field for features
	 */
	public static final String ID_FIELD = "cabd_id";

    private Logger logger = LoggerFactory.getLogger(FeatureDao.class);
    
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private FeatureTypeManager typeManager;
	
	
	/**
	 * Finds the feature wtih the given uuid.  Will return null
	 * if no feature is found.
	 * 
	 * @param uuid
	 * @return
	 */
	public Feature getFeature(UUID uuid) {
		String type = null;
		try {
			String query = "SELECT feature_type FROM " + FeatureViewMetadata.ALL_FEATURES_VIEW + " WHERE " + ID_FIELD + " = ? ";
			type = jdbcTemplate.queryForObject(query, String.class, uuid);
		}catch(EmptyResultDataAccessException ex) {
			return null;
		}
		
		FeatureType btype = typeManager.getFeatureType(type);
		if (btype == null) {
			logger.error(MessageFormat.format("Database Error: No entry for feature type ''{0}'' in the feature_types database table.",type));
			return null;
		}
		
		
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		sb.append( ID_FIELD );
		btype.getViewMetadata().getFields().forEach(e->{
			if (!e.isGeometry()) {
				sb.append("," + e.getFieldName() ); 
			}else {
				sb.append(", st_asbinary(" + e.getFieldName() + ") as " + e.getFieldName() );
			}
		} );
		sb.append(" FROM " );
		sb.append(btype.getViewMetadata().getFeatureView());
		sb.append(" WHERE ");
		sb.append(ID_FIELD);
		sb.append(" = ? ");
		
		try {
			return jdbcTemplate.queryForObject(sb.toString(), new FeatureRowMapper(btype.getViewMetadata()), uuid);
		}catch (EmptyResultDataAccessException ex) {
			return null;
		}
	}
	
	/**
	 * Get all features of the given types within the given envelope.
	 * Both parameters can be null.  This will only return attributes
	 * that are shared across all feature types.
	 * 
	 * @param types set of feature types to search, can be null
	 * @param env the envelope to search, can be null
	 * @return
	 */
	public List<Feature> getFeatures(List<FeatureType> types, Envelope env, int maxresults) {
		FeatureViewMetadata vmetadata = typeManager.getAllViewMetadata();
		return getFeaturesInternal(vmetadata, types, env, maxresults);
	}
	
	/**
	 * Get all features of the given type within the envelope.
	 * 
	 * This will return all attributes for the schema associated with the type.
	 * 
	 * @param type the feature type, must be provided
	 * @param env the envelope to search, can be null
	 * @return
	 */
	public List<Feature> getFeatures(FeatureType type, Envelope env, int maxresults) {
		FeatureViewMetadata vmetadata = type.getViewMetadata();
		return getFeaturesInternal(vmetadata, null, env, maxresults);
	}
	
	/**
	 * Returns the N-nearest features of the given type to the point.  
	 * This will return all attribute for the schema associated with the
	 * feature type.
	 * 
	 * @param type the feature type, must be provided
	 * @param pnt the center point
	 * @param maxResults the maximum number of results to return
	 * @return
	 */
	public List<Feature> getFeatures(FeatureType type, Coordinate pnt, Integer maxResults) {
		FeatureViewMetadata vmetadata = type.getViewMetadata();
		return getFeaturesInternal(vmetadata, null, pnt, maxResults);
	}
	
	/**
	 * Returns the N-nearest features of the given types to the point.  
	 * This will return only attributes shared accross all feature types.
	 * 
	 * @param types the feature types to search
	 * @param pnt the center point
	 * @param maxResults the maximum number of results to return
	 * @return
	 */
	public List<Feature> getFeatures(List<FeatureType> types, Coordinate pnt, Integer maxResults) {
		FeatureViewMetadata vmetadata = typeManager.getAllViewMetadata();
		return getFeaturesInternal(vmetadata, types, pnt, maxResults);
	}
	
	/*
	 * Searches metadata based on envelope
	 */
	private List<Feature> getFeaturesInternal(FeatureViewMetadata vmetadata,
			List<FeatureType> types,
			Envelope env, int maxresults){
		
		String geomField = null;
		
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		sb.append(ID_FIELD);

		for (FeatureViewMetadataField field : vmetadata.getFields()) {
			if (!field.isGeometry()) {
				sb.append("," + field.getFieldName() );
			}else {
				geomField = field.getFieldName();
				sb.append(", st_asbinary(" + field.getFieldName() + ") as " + field.getFieldName() );
			}
		}
		
		sb.append(" FROM " );
		sb.append(vmetadata.getFeatureView());
		
		if ((types != null && !types.isEmpty()) || 
				(env != null && geomField != null)) {
			sb.append(" WHERE ");
		}
		
		String[] btypes = null;
		if (types != null && !types.isEmpty()) {
		
			btypes = new String[types.size()];
			for (int i = 0; i < types.size(); i ++) btypes[i] = types.get(i).getType();
			
			sb.append(" feature_type IN (");
			sb.append(String.join(",", Collections.nCopies(types.size(), "?")));
			sb.append(")");
			
			if (env != null && geomField != null) sb.append(" AND ");
		}
		
		if (env != null && geomField != null) {
			sb.append(" st_intersects(");
			sb.append( geomField);
			sb.append(", ST_MakeEnvelope(");
			sb.append(env.getMinX());
			sb.append(",");
			sb.append(env.getMinY());
			sb.append(",");
			sb.append(env.getMaxX());
			sb.append(",");
			sb.append(env.getMaxY());
			sb.append(", ");
			sb.append(DATABASE_SRID);
			sb.append(" ) )" );
		}
		
		sb.append(" LIMIT ");
		sb.append(maxresults);
		
		List<Feature> features = 
				jdbcTemplate.query(sb.toString(), 
						new FeatureRowMapper(vmetadata), btypes);
		return features;
	}
	
	
	//nearest neighbour searching
	//https://postgis.net/workshops/postgis-intro/knn.html
	private List<Feature> getFeaturesInternal(FeatureViewMetadata vmetadata,
			List<FeatureType> types,
			Coordinate c, Integer maxResults){
		
		String geomField = null;
		
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		sb.append(ID_FIELD);

		for (FeatureViewMetadataField field : vmetadata.getFields()) {
			if (!field.isGeometry()) {
				sb.append("," + field.getFieldName() );
			}else {
				geomField = field.getFieldName();
				sb.append(", st_asbinary(" + field.getFieldName() + ") as " + field.getFieldName() );
			}
		}
		
		if (geomField == null) {
			throw new InvalidDatabaseConfigException(MessageFormat.format("Not geometry column found for the view ''{0}''", vmetadata.getFeatureView()));
		}
		
		sb.append(" FROM " );
		sb.append(vmetadata.getFeatureView());
		
		String[] btypes = null;
		if (types != null && !types.isEmpty()) {
			btypes = new String[types.size()];
			for (int i = 0; i < types.size(); i ++) btypes[i] = types.get(i).getType();
			
			sb.append(" WHERE ");	
			sb.append(" feature_type IN (");
			sb.append(String.join(",", Collections.nCopies(types.size(), "?")));
			sb.append(")");
		}
		
		sb.append(" ORDER BY ");
		sb.append(geomField);
		sb.append(" <-> ");
		sb.append(" st_setsrid(st_makepoint(" + c.x + "," + c.y + "),");
		sb.append(DATABASE_SRID);
		sb.append(")");
		sb.append(" LIMIT " );
		sb.append(maxResults);
			
		List<Feature> features = 
				jdbcTemplate.query(sb.toString(), 
						new FeatureRowMapper(vmetadata), btypes);
		return features;
	}
}
