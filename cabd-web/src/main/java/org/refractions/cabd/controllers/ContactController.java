/*
 * Copyright 2022 Canadian Wildlife Federation
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
package org.refractions.cabd.controllers;

import java.security.InvalidParameterException;

import javax.servlet.http.HttpServletRequest;

import org.refractions.cabd.dao.ContactDao;
import org.refractions.cabd.model.Contact;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST api for Contacts
 * 
 * @author Emily
 *
 */
@RestController
@RequestMapping("/" + ContactController.PATH)
public class ContactController {

	public static final String PATH = "contacts";
	
	@Autowired
	ContactDao contactDao;
		
	/**
	 * checks if a contact already exists; updates it if it exits or 
	 * creates a new one if it doesn't exist
	 * 
	 * @param contact
	 * @param request
	 * @return
	 */
	//requires content-type = application/json in request
	@PutMapping
	public ResponseEntity<Contact> putContact( 
			@RequestBody Contact contact,
			HttpServletRequest request) {
		
		String error = Contact.validateEmail(contact.getEmail());
		if (error != null) throw new InvalidParameterException(error);
		
		if (contact.getName() == null || contact.getName().isBlank()) throw new InvalidParameterException("Name is required for contacts.");
		
		Contact c = contactDao.getUpdateOrCreateContact(contact.getEmail(), 
				contact.getName(), contact.getOrganization(), contact.getMailinglist());
		
		return ResponseEntity.ok(c);
	}
	
	/**
	 * Get only the mailing perference for a given email 
	 */
	@GetMapping(produces=MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<Object> getContact( 
			@RequestParam(name="email", required=true) String email) {
		
		if (email == null) return ResponseEntity.badRequest().build();
		
		Contact c = contactDao.getContact(email);
		if (c == null) return ResponseEntity.ok("{\"mailinglist\": true}");
		
		String json = "{\"mailinglist\": " + (c.getMailinglist() ? "true" : "false") + "}";
		return ResponseEntity.ok(json);
	}
}
