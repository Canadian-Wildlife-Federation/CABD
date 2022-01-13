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

import java.time.LocalDate;

import org.locationtech.jts.geom.Geometry;

/**
 * Contains details about a feature
 * view field.
 * 
 * @author Emily
 *
 */
public class FeatureViewMetadataField {

	private String fieldName;
	private String name;
	private String description;

	private String datatype;
	private Integer simpleOrder;
	private Integer allOrder;
	
	private boolean isLink = false;
	
	private boolean isGeometry = false;
	private Integer srid = null;
	
	public FeatureViewMetadataField(String fieldName, String name, String description, 
			boolean isLink, String datatype, Integer simpleOrder, Integer allOrder) {
		this.fieldName = fieldName;
		this.name = name;
		this.description = description;
		this.isLink = isLink;
		this.datatype = datatype;
		this.simpleOrder = simpleOrder;
		this.allOrder = allOrder;
	}
	
	public String getDataType() {
		return this.datatype;
	}
	
	public Class<?> getDataTypeAsClass(){
		String ldt = datatype.toLowerCase();
		if (ldt.equals("boolean")) return Boolean.class;
		if (ldt.equals("integer")) return Integer.class;
		if (ldt.equals("double")) return Double.class;
		if (ldt.equals("date")) return LocalDate.class;
		if (ldt.equals("geometry")) return Geometry.class;
		return String.class;
	}
	
	
	public Integer getAllOrder() {
		return this.allOrder;
	}
	
	public Integer getSimpleOrder() {
		return this.simpleOrder;
	}
	public boolean isLink() {
		return this.isLink;
	}
	
	public String getFieldName() {
		return fieldName;
	}
	public String getName() {
		return name;
	}
	public String getDescription() {
		return description;
	}
	
	public boolean isGeometry() {
		return this.isGeometry;
	}
	
	public void setGeometry(boolean geometry, Integer srid) {
		this.isGeometry = geometry;
		this.srid = srid;
	}
	
	public Integer getSRID() {
		return this.srid;
	}
	
}
