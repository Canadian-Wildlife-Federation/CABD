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
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.refractions.cabd.CabdConfigurationProperties;
import org.refractions.cabd.model.CommunityContact;
import org.refractions.cabd.model.CommunityData;
import org.refractions.cabd.model.CommunityFeature;
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
		new CommunityContact((UUID)rs.getObject("user_id"), rs.getString("username")); 
	
	/**
	 * Mapper for community data - only maps id, data, uploaded_datetime, and status 
	 * fields
	 */
	private RowMapper<CommunityData> communityDataMapper = (rs, rownum)-> 
		new CommunityData((UUID)rs.getObject("id"), 
				rs.getString("data"),
				rs.getTimestamp("uploaded_datetime").toInstant(),
				rs.getString("status"));

	 
	private RowMapper<CommunityData> communityDataMapperNoData = (rs, rownum)-> 
		new CommunityData((UUID)rs.getObject("id"), 
				rs.getTimestamp("uploaded_datetime").toInstant(),
				rs.getString("status"),
				rs.getString("status_message"),
				(String[])((Array)rs.getObject("warnings")).getArray());	  
		
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
		sb.append(" (uploaded_datetime, data)");
		sb.append(" VALUES (?, ?) ");
		sb.append(" RETURNING id ");

		
		UUID id = jdbcTemplate.queryForObject(sb.toString(),UUID.class, Timestamp.from( data.getUploadeddatetime() ), data.getData());
		data.setId(id);
	}
	
	
	/**
	 * Find a contact with the given email address or return null if none found
	 * @param email
	 * @return
	 */
	public CommunityContact getCommunityContact(String username) {
		try {
			String query = "SELECT user_id, username FROM  " + COMMUNITY_CONTACT_TABLE + " WHERE username = ? ";
			return jdbcTemplate.queryForObject(query, contactTypeMapper, username.toLowerCase());
		}catch(EmptyResultDataAccessException ex) {
			return null;
		}
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
	public CommunityContact getOrCreateCommunityContact(String username) {
		CommunityContact c = getCommunityContact(username);
		if (c == null) {
			String insert = "INSERT INTO " + COMMUNITY_CONTACT_TABLE + "(username) VALUES (?)";
			jdbcTemplate.update(insert, username.toLowerCase());
			c = getCommunityContact(username);
		}
		return c;
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
		sb.append("SELECT id, uploaded_datetime, status, status_message, warnings FROM ");
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
}
