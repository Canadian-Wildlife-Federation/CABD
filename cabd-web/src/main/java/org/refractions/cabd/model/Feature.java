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
package org.refractions.cabd.model;

import java.util.HashMap;
import java.util.UUID;

import org.locationtech.jts.geom.Geometry;

/**
 * Represents a Feature in the system which has
 * and identifier, geometry and set of attributes.
 * 
 * @author Emily
 *
 */
public class Feature {

	private Geometry geom;
	
	private UUID id;
	private HashMap<String, Object> attributes;
	private HashMap<String, UUID> links;

	public Feature(UUID id) {
		this.id = id;
		attributes = new HashMap<>();
		links = new HashMap<>();
	}

	public UUID getId() {
		return this.id;
	}

	public void addAttribute(String key, Object value)  {
		attributes.put(key, value);
	}

	public void addLinkAttribute(String key, UUID value)  {
		links.put(key, value);
	}
	public Object getAttribute(String key) {
		return attributes.get(key);
	}
	public HashMap<String, Object> getAttributes() {
		return attributes;
	}
	
	public HashMap<String, UUID> getLinkAttributes() {
		return links;
	}
	
	public void setGeometry(Geometry geometry) {
		this.geom = geometry;
	}
	
	public Geometry getGeometry() {
		return this.geom;
	}

}
