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
package org.refractions.cabd.dao;

import java.util.Collection;
import java.util.List;

import javax.annotation.PostConstruct;

import org.refractions.cabd.model.assessment.AssessmentType;
import org.refractions.cabd.model.assessment.AssessmentTypeMetadataField;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Object to manage assessment types configured in 
 * database.  This object is populated on startup
 * and the details are cached.  If the metadata changes
 * in the database, the server needs to be restarted.
 * 
 * @author Emily
 *
 */
@Service
public class AssessmentTypeManager {

	@Autowired
	private AssessmentTypeDao typeDao;
	
	//cached types and metadata
	private List<AssessmentType> types;
	
	/**
	 * Sets assessment types
	 * @param types
	 */
	private void setAssessmentTypes(List<AssessmentType> types) {
		this.types = types;
	}
	
	/**
	 * Gets all assessment types
	 * 
	 * @return
	 */
	public List<AssessmentType> getAssessmentTypes(){
		return this.types;
	}

	
	/**
	 * 
	 * @param assessmentType
	 * @return true if the type is registered in the database otherwise false
	 */
	public boolean isValidType(String assessmentType) {
		for (AssessmentType t : types) if (t.getType().equalsIgnoreCase(assessmentType)) return true;
		return false;
	}
	
	/**
	 * Find the assessment type object for the given type.
	 * @param assessmentType
	 * @return
	 */
	public AssessmentType getAssessmentType(String assessmentType) {
		for (AssessmentType t : types) {
			if (t.getType().equalsIgnoreCase(assessmentType)) return t;
		}
		return null;
	}
	
	/**
	 * Initializes the cache from the database
	 * on startup.
	 */
	@PostConstruct
    public void init() {
		LoggerFactory.getLogger(getClass()).info("Loading Assessment Type Metadata");
        
		this.setAssessmentTypes( typeDao.getFeatureTypes() );      
        
        for (AssessmentType t : this.types) {
        	Collection<AssessmentTypeMetadataField> sites = typeDao.getTypeMetadata(t.getType());
        	Collection<AssessmentTypeMetadataField> structures = typeDao.getTypeMetadata(t.getType() + ".structure");
        	t.setViewMetadata( sites, structures);
        }
        
        LoggerFactory.getLogger(getClass()).info("Loading Assessment Type Metadata ... Complete");
    }

}
