package org.refractions.cabd.dao;

import java.sql.Types;
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
				rs.getString("name"), rs.getString("organization"), (UUID)rs.getObject("datasource_id")); 
				
	
	public Contact getContact(String email) {
		try {
			String query = "SELECT id, email, name, organization, datasource_id FROM  " + TABLE + " WHERE email = ? ";
			return jdbcTemplate.queryForObject(query, contactTypeMapper, email);
		}catch(EmptyResultDataAccessException ex) {
			return null;
		}
	}
	
	public Contact getUpdateOrCreateContact(String email, String name, String organization) {
		Contact c = getContact(email);
		if (c == null) {
			c = new Contact(email, name, organization);
			//save
			String insert = "INSERT INTO " + TABLE + "(email, name, organization) VALUES (?, ?, ?)";
			jdbcTemplate.update(insert, email, name, organization);
			c = getContact(email);
		}else {
			if (!c.getName().equals(name) || !c.getOrganization().equals(organization)) {
				//update
				c.setName(name);
				c.setOrganization(organization);
				//save
				String update = "UPDATE " + TABLE + " SET name = ?, organization = ? WHERE id = ? ";
				jdbcTemplate.update(update, name, organization, c.getId());
			}
		}
		return c;
	}
		
}
