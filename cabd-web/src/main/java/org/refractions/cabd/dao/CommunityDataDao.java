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

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;
import org.refractions.cabd.CabdConfigurationProperties;
import org.refractions.cabd.model.CommunityContact;
import org.refractions.cabd.model.CommunityData;
import org.refractions.cabd.model.CommunityFeature;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.SimpleFeatureList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

/**
 * Manage community data 
 * 
 * @author Emily
 *
 */
@Component
public class CommunityDataDao {

    private static final String COMMUNITY_DATA_TABLE = "cabd.community_data_raw";
    
    private static final String COMMUNITY_CONTACT_TABLE = "cabd.community_contact";
    
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	
	@Autowired
	CabdConfigurationProperties properties;
	
	/**
	 * Mapper for contact  
	 */
	private RowMapper<CommunityContact> contactTypeMapper = (rs, rownum)-> 
		new CommunityContact((UUID)rs.getObject("user_id"), rs.getString("username"), rs.getString("oauth_id")); 
	
	/**
	 * Mapper for community data - only maps id, data, uploaded_datetime, and status 
	 * fields
	 */
	private RowMapper<CommunityData> communityDataMapper = (rs, rownum)-> 
		new CommunityData((UUID)rs.getObject("id"), 
				rs.getString("data"),
				rs.getTimestamp("uploaded_datetime").toInstant(),
				rs.getString("status"),
				rs.getString("oauth_id"),
				rs.getString("oauth_email"));

	 
	private RowMapper<CommunityData> communityDataMapperNoData = (rs, rownum)->
		new CommunityData((UUID)rs.getObject("id"), 
				rs.getTimestamp("uploaded_datetime").toInstant(),
				rs.getString("status"),
				rs.getString("status_message"),
				rs.getObject("warnings") == null ? new String[] {} : (String[])((Array)rs.getObject("warnings")).getArray(),
				rs.getString("oauth_id"),
				rs.getString("oauth_email"));	  
		
	private WKBReader reader = new WKBReader();

	private RowMapper<Feature> ghostFeatureMapper = (rs, rownum)-> 
	{
		UUID cabdId = (UUID) rs.getObject(2);
		String fType = rs.getString(1);
		Feature gFeature = new Feature(cabdId, fType);
		
		try {
			Point pnt = (Point) reader.read(rs.getBytes(3));
			gFeature.setGeometry(pnt);
		}catch (Exception ex) {
			throw new SQLException(ex);
		}
		return gFeature;
	};
	
	/**
	 * Saves the raw community data and assigned id from database.
	 * 
	 * @param uuid
	 * @return
	 */
	public void saveRawData(CommunityData data) {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(COMMUNITY_DATA_TABLE);
		sb.append(" (uploaded_datetime, data, oauth_id, oauth_email)");
		sb.append(" VALUES (?, ?, ?, ?) ");
		sb.append(" RETURNING id ");

		
		UUID id = jdbcTemplate.queryForObject(sb.toString(),UUID.class, Timestamp.from( data.getUploadeddatetime() ), data.getData(), data.getOAuthId(), data.getOAuthEmail());
		data.setId(id);
	}
	
	/**
	 * finds the contact associated with the given aouthid or null if not found
	 * @param oauthId
	 * @return
	 */
	public CommunityContact findCommunityContact(String oauthId) {
		if (oauthId == null) return null;
		
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT * FROM ");
		sb.append(COMMUNITY_CONTACT_TABLE);
		sb.append(" WHERE oauth_id = ?");
		try {
			return jdbcTemplate.queryForObject(sb.toString(), contactTypeMapper, oauthId);
		}catch (EmptyResultDataAccessException ex) {				
		}
		return null;		
	}
	
	
	/**
	 * Finds the community contact with the username. If no contact is found
	 * a new contact is created.
	 * 
	 * @param email
	 * @param name
	 * @param organization
	 * @return
	 */
	public CommunityContact getOrCreateCommunityContact(String oauthId, String oauthEmail, String dataUsername) {
		//1. find user with same oauthid		
		//2. find user with the same oauth email - make assumptions that this is the same user		
		//3. find user with username 		
		//4. create new user
		String querypart = "SELECT user_id, username, oauth_id FROM  " + COMMUNITY_CONTACT_TABLE + " WHERE ";
		
		if (oauthId != null) {
			String query = querypart + "oauth_id = ?";
			try {
				return jdbcTemplate.queryForObject(query, contactTypeMapper, oauthId);
			}catch (EmptyResultDataAccessException ex) {				
			}
		}
		
		if (oauthEmail != null) {
			String query = querypart + "username = ?";
			try {
				CommunityContact c = jdbcTemplate.queryForObject(query, contactTypeMapper, oauthEmail);
				return updateCommunityContact(c, oauthId);
			}catch (EmptyResultDataAccessException ex) {
			}
		}
		
		if (dataUsername != null) {
			String query = querypart + "username = ?";
			try {
				CommunityContact c = jdbcTemplate.queryForObject(query, contactTypeMapper, dataUsername);
				return updateCommunityContact(c, oauthId);
			}catch (EmptyResultDataAccessException ex) {
			}
		}
		
		//no contact, lets create a new one		
		String insert = "INSERT INTO " + COMMUNITY_CONTACT_TABLE + "(username, oauth_id) VALUES (?, ?) RETURNING user_id, username, oauth_id";
		CommunityContact c = jdbcTemplate.queryForObject(insert, contactTypeMapper, dataUsername != null ? dataUsername : oauthEmail, oauthId);
		
		return c;
	}
	
	private CommunityContact updateCommunityContact(CommunityContact c, String oauthId) {
		String insert = "UPDATE " + COMMUNITY_CONTACT_TABLE + " set oauth_id = ? where user_id = ? RETURNING user_id, username, oauth_id";
		return jdbcTemplate.queryForObject(insert, contactTypeMapper, oauthId, c.getId());
		
	}

	/**
	 * Saves the community feature to the appropriate data type
	 * 
	 * @param dataTable datatable to write to
	 * @param feature community feature to write
	 */
	public void saveCommunityFeature(String dataTable, CommunityFeature feature) {
		StringBuilder sb = new StringBuilder();
		sb.append(" INSERT INTO ");
		sb.append(dataTable);
		sb.append(" (id, cabd_id, uploaded_datetime, user_id, data)");
		sb.append("VALUES(?,?,?,?,?::jsonb)");
		
		jdbcTemplate.update(sb.toString(), 
				feature.getId(),
				feature.getCabdId(),
				Timestamp.from( feature.getRawData().getUploadeddatetime()),
				feature.getCommunityContact().getId(),
				feature.getJsonDataAsString());
	}
	
	/**
	 * Finds the next raw community data to process and "checks" it out
	 * of the database.
	 * 
	 * @return
	 */
	public CommunityData checkOutNext() {
	
		StringBuilder sb = new StringBuilder();
		sb.append("WITH uprow AS (");
		sb.append("SELECT * FROM ");
		sb.append(COMMUNITY_DATA_TABLE);
		sb.append(" WHERE status = ? LIMIT 1 ");
		sb.append(")");
		sb.append("UPDATE " );
		sb.append(COMMUNITY_DATA_TABLE);
		sb.append(" SET status = ? FROM uprow ");
		sb.append(" WHERE uprow.id = " );
		sb.append(COMMUNITY_DATA_TABLE);
		sb.append(".id RETURNING ");
		sb.append(COMMUNITY_DATA_TABLE);
		sb.append(".*");
		
		try(Stream<CommunityData> items = jdbcTemplate.queryForStream(sb.toString(), communityDataMapper, CommunityData.Status.NEW.name(), CommunityData.Status.PROCESSING.name())){
			Optional<CommunityData> first = items.findFirst();
			if (first.isEmpty()) return null;
			return first.get();
		}
	}
	
	/**
	 * This is intended for testing purposes only.
	 * 
	 * Gets the community data status from the database for a given id.
	 * Excludes the data field from the results. 
	 * @param id
	 * @return
	 */
	public CommunityData getCommunityDataRaw(UUID id) {
		
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT id, uploaded_datetime, status, status_message, warnings, oauth_id, oauth_email FROM ");
		sb.append(COMMUNITY_DATA_TABLE);
		sb.append(" WHERE id = ? ");
		
		List<CommunityData> data = jdbcTemplate.query(sb.toString(), communityDataMapperNoData, id);
		if (data.isEmpty()) return null;
		return data.get(0);
	}
	
	
	/**
	 * Updates the status, message, and warnings associated with the community data field.  
	 * @param data
	 */
	public void updateStatus(CommunityData data) {
		
		StringBuilder sb = new StringBuilder();
		sb.append(" UPDATE ");
		sb.append(COMMUNITY_DATA_TABLE);
		sb.append(" SET status = ?, status_message = ?, warnings = ?");
		sb.append(" WHERE id = ? ");
		
		jdbcTemplate.update(sb.toString(), 
				data.getStatus().name(),
				data.getStatusMessage(),
				data.getWarningsArray(),
				data.getId());
	}
	
	/**
	 * Delete the raw community data
	 *   
	 * @param data
	 */
	public void deleteRawData(CommunityData data) {
		
		StringBuilder sb = new StringBuilder();
		sb.append(" DELETE FROM ");
		sb.append(COMMUNITY_DATA_TABLE);
		sb.append(" WHERE id = ? ");
		
		jdbcTemplate.update(sb.toString(), data.getId());
	}
	
	
	public SimpleFeatureList getGhostFeatures(Collection<FeatureType> fTypes, UUID userId){
		
		
		boolean hasCd = false;
		StringBuilder sb = new StringBuilder();
		List<UUID> params = new ArrayList<>();
		for (FeatureType fType : fTypes) {
			if (fType.getCommunityDataTable() == null || fType.getCommunityDataTable().isBlank()) continue;
			
			if (sb.length() > 0) {
				sb.append(" UNION ");
			}
			hasCd = true;
			sb.append(" SELECT '" + fType.getType() + "', cabd_id, st_asewkb(st_geomfromgeojson(data->'geometry'))");
			sb.append(" FROM ");
			sb.append(fType.getCommunityDataTable());
			sb.append(" f WHERE user_id = ? AND status = 'NEW' AND NOT EXISTS (SELECT 1 FROM ");
			sb.append( FeatureViewMetadata.getAllFeaturesView() );
			sb.append(" a WHERE a.cabd_id = f.cabd_id )");
			params.add(userId);
		}
		sb.append(" LIMIT 5000" );
		
		if (!hasCd) return new SimpleFeatureList(Collections.emptyList());
		
		return new SimpleFeatureList(jdbcTemplate.query(sb.toString(), ghostFeatureMapper, params.toArray(new Object[0])));
		
	}
}
