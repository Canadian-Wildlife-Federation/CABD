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

import java.util.Collection;

import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.model.NamedItem;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Represents a assessment type.
 * 
 * @author Emily
 *
 */
public class AssessmentType extends NamedItem{

	/**
	 * These are the types used for source types in the database. We haven't created
	 * database types for modelled crossings or satellite as those are not required.
	 * 
	 * @author Emily
	 *
	 */
	public enum RawAssessmentType{
		MODELLED_CROSSINGS("m", "modelled-crossings"),
		SATELLITE("s", "satellite-review"),
		COMMUNITY("c", "rapid"),
		ASSESSMENT("a", "long-form");
		
		private String dbkey;
		private String datasourcename;
		
		RawAssessmentType(String dbkey, String datasourcename){
			this.dbkey = dbkey;
			this.datasourcename = datasourcename;
		}
		
		public static RawAssessmentType findType(String dbkey) {
			dbkey = dbkey.toLowerCase();
			for(RawAssessmentType t : values()) {
				if (t.dbkey.equals(dbkey)) return t; 
			}
			return null;
		}
		
		public String getDataSourceName() {
			return this.datasourcename;
		}
	}
	
	
	private String type;
	private String dataView;
	
	private String metadataUrl;
	
	private Collection<AssessmentTypeMetadataField> sitefields;
	private Collection<AssessmentTypeMetadataField> structurefields;

	public AssessmentType(String type, String dataView, String name_en, String name_fr) {
		super(name_en, name_fr);
		this.type = type;
		this.dataView = dataView;
	}
	
	public String getType() {
		return type;
	}

	public String getMetadataUrl() {
		return this.metadataUrl;
	}
	
	public void setUrls(String metadataUrl) {
		this.metadataUrl = metadataUrl + "/" + type;
	}
	/**
	 * Returns the "core" name of the view containing the metadata
	 * for the feature type. This is the name referenced in the metadata table.
	 *  If you want the actual view with the data
	 * use the getDataView() function
	 * @return
	 */
	@JsonIgnore
	public String getDataViewName() {
		return this.dataView;
	}
	
	/**
	 * Returns the name of the view in the database containing the data
	 * for this feature type AND the request local
	 * @return
	 */
	@JsonIgnore
	public String getDataView() {
		if (CabdApplication.isFrench()) {
			return this.dataView + "_fr";
		}
		return this.dataView + "_en";
	}

	@JsonIgnore
	public Collection<AssessmentTypeMetadataField> getSiteAttributes() {
		return this.sitefields;
	}

	@JsonIgnore
	public Collection<AssessmentTypeMetadataField> getStructureAttributes() {
		return this.structurefields;
	}
	
	public void setViewMetadata(Collection<AssessmentTypeMetadataField> sitefields, Collection<AssessmentTypeMetadataField> structurefields) {
		this.sitefields = sitefields;
		this.structurefields = structurefields;
	}
	
}
