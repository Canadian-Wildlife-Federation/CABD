/*
 * Copyright 2025 Canadian Wildlife Federation
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
package org.refractions.cabd.model.assessment;

import java.sql.Date;

import org.locationtech.jts.geom.Geometry;
import org.refractions.cabd.model.NamedDescriptionItem;

/**
 * Contains details about a feature
 * view field.
 * 
 * @author Emily
 *
 */
public class AssessmentTypeMetadataField extends NamedDescriptionItem {

	private String fieldName;
	private String datatype;
	
	
	public AssessmentTypeMetadataField(String fieldName, String name_en, String description_en,
			String name_fr, String description_fr,
			String datatype) {
		super(name_en, name_fr, description_en, description_fr);
		this.fieldName = fieldName;
		this.datatype = datatype;	
	}
	
	public String getDataType() {
		return this.datatype;
	}
	
	
	public Class<?> getDataTypeAsClass(){
		String ldt = datatype.toLowerCase();
		if (ldt.equals("boolean")) return Boolean.class;
		if (ldt.equals("integer")) return Integer.class;
		if (ldt.equals("double")) return Double.class;
		if (ldt.equals("date")) return Date.class;
		if (ldt.equals("geometry")) return Geometry.class;
		return String.class;
	}
	
	public String getFieldName() {
		return fieldName;
	}	
}
