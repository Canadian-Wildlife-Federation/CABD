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

import java.sql.Date;
import java.util.List;
import java.util.Map;

import org.locationtech.jts.geom.Geometry;
import org.refractions.cabd.controllers.AttributeSet;

/**
 * Contains details about a feature
 * view field.
 * 
 * @author Emily
 *
 */
public class FeatureViewMetadataField extends NamedDescriptionItem {

	private String fieldName;
	
	private String datatype;
	private String valueOptionsRef;
	
	private boolean isLink = false;
	private boolean includeVectorTile = false;
	
	private boolean isGeometry = false;
	private Integer srid = null;
	private boolean isNameSearch = false;
	
	private String shapeFieldName;
	
	private List<FeatureTypeListValue> valueOptions;
	//mapping between attribute set and the order
	//the attribute should appears in the results
	private Map<String,Integer> attributeSetMapping;
	
	public FeatureViewMetadataField(String fieldName, String name_en, String description_en,
			String name_fr, String description_fr,
			boolean isLink, String datatype, 
			boolean includeVectorTile, String validValues, 
			boolean isNameSearch, String shapeFieldName,
			Map<String,Integer> attributeSetMapping) {
		super(name_en, name_fr, description_en, description_fr);
		this.fieldName = fieldName;
		this.isLink = isLink;
		this.datatype = datatype;
		this.includeVectorTile = includeVectorTile;
		this.valueOptionsRef = validValues;
		this.valueOptions = null;
		this.isNameSearch = isNameSearch;
		this.shapeFieldName = shapeFieldName;
		this.attributeSetMapping = attributeSetMapping;
	}
	
	public boolean isNameSearch() {
		return this.isNameSearch;
	}
	
	public boolean includeVectorTile() {
		return this.includeVectorTile;
	}
	public String getDataType() {
		return this.datatype;
	}
	
	public List<FeatureTypeListValue> getValueOptions(){
		return this.valueOptions;
	}
	
	public void setValueOptions(List<FeatureTypeListValue>  valueOptions){
		this.valueOptions = valueOptions;
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
	
	public String getValidValuesReference() {
		return this.valueOptionsRef;
	}
	
	public Integer getOrder(AttributeSet s) {
		return this.attributeSetMapping.get(s.getName());
	}
	
	public boolean isLink() {
		return this.isLink;
	}
	
	public String getFieldName() {
		return fieldName;
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
	
	public String getShapefileFieldName() {
		return this.shapeFieldName;
	}
}
