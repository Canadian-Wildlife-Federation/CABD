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

import java.util.UUID;

import org.refractions.cabd.model.Contact;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

@Component
public class ContactDao {

	private static final String TABLE = "cabd.contacts";
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	
	/**
	 * Mapper for contact  
	 */
	private RowMapper<Contact> contactTypeMapper = (rs, rownum)-> 
		new Contact((UUID)rs.getObject("id"), rs.getString("email"),
				rs.getString("name"), rs.getString("organization"), 
				(UUID)rs.getObject("datasource_id"), rs.getBoolean("mailing_list")); 
				
	
	/**
	 * Find a contact with the given email address or return null if none found
	 * @param email
	 * @return
	 */
	public Contact getContact(String email) {
		try {
			String query = "SELECT id, email, name, organization, datasource_id, mailing_list FROM  " + TABLE + " WHERE email = ? ";
			return jdbcTemplate.queryForObject(query, contactTypeMapper, email);
		}catch(EmptyResultDataAccessException ex) {
			return null;
		}
	}
	
	/**
	 * Finds the contact with the email and updates the information. If no contact is found
	 * a new contact is created.
	 * 
	 * @param email
	 * @param name
	 * @param organization
	 * @return
	 */
	public Contact getUpdateOrCreateContact(String email, String name, String organization, Boolean isMailingList) {
		Contact c = getContact(email);
		if (c == null) {
			c = new Contact(email, name, organization, isMailingList);
			//save
			if (c.getMailinglist() != null) {
				String insert = "INSERT INTO " + TABLE + "(email, name, organization, mailing_list) VALUES (?, ?, ?, ?)";
				jdbcTemplate.update(insert, c.getEmail(), c.getName(), c.getOrganization(), c.getMailinglist());
			}else {
				String insert = "INSERT INTO " + TABLE + "(email, name, organization) VALUES (?, ?, ?)";
				jdbcTemplate.update(insert, c.getEmail(), c.getName(), c.getOrganization());
			}
			c = getContact(email);
		}else {
			if (!nullequals(c.getName(), name) 
					|| !nullequals(c.getOrganization(), organization) ||
					(isMailingList != null && !isMailingList.equals(c.getMailinglist()))
					) {
				//update
				c.setName(name);
				c.setOrganization(organization);
				if (isMailingList != null) {
					//only update mailing list if value is provided
					c.setMailinglist(isMailingList);
				}
				//save
				String update = "UPDATE " + TABLE + " SET name = ?, organization = ?, mailing_list = ? WHERE id = ? ";
				jdbcTemplate.update(update, c.getName(), c.getOrganization(), c.getMailinglist(), c.getId());
			}
		}
		return c;
	}
	
	private boolean nullequals(String a, String b) {
		if (a == null && b == null) return true;
		if (a == null && b != null) return false;
		if (a != null && b == null) return false;
		return a.equals(b);
	}
}
