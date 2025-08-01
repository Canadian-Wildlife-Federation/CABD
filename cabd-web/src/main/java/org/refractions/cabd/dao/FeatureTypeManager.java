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

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.refractions.cabd.controllers.AttributeSet;
import org.refractions.cabd.exceptions.InvalidDatabaseConfigException;
import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;
import org.refractions.cabd.model.FeatureViewMetadataFieldData;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Object to manage feature types configured in 
 * database.  This object is populated on startup
 * and the details are cached.  If the metadata changes
 * in the database, the server needs to be restarted.
 * 
 * @author Emily
 *
 */
@Service
public class FeatureTypeManager {

	/**
	 * These two feature types have some special handing for data sources
	 */
	public static final String SITE_FEATURE_TYPE = "sites";
	public static final String STRUCTURE_FEATURE_TYPE = "structures";
	
	@Autowired
	private FeatureTypeDao typeDao;
	
	//cached types and metadata
	private List<FeatureType> types;
	private FeatureViewMetadata allViewMetadata;
	private List<AttributeSet> attributeSets;
	
	/**
	 * Sets feature types
	 * @param types
	 */
	private void setFeatureTypes(List<FeatureType> types) {
		this.types = types;
	}
	
	/**
	 * Gets all feature types
	 * 
	 * @return
	 */
	public List<FeatureType> getFeatureTypes(){
		return this.types;
	}
	
	/**
	 * Gets all attribute sets configured in the database
	 * 
	 * @return
	 */
	public List<AttributeSet> getAttributeSets(){
		return this.attributeSets;
	}
	
	/**
	 * 
	 * @param featureType
	 * @return true if the type is registered in the database otherwise false
	 */
	public boolean isValidType(String featureType) {
		for (FeatureType t : types) if (t.getType().equalsIgnoreCase(featureType)) return true;
		return false;
	}
	
	/**
	 * Find the feature type object for the given feature type.
	 * @param featureType
	 * @return
	 */
	public FeatureType getFeatureType(String featureType) {
		for (FeatureType t : types) {
			if (t.getType().equalsIgnoreCase(featureType)) return t;
		}
		return null;
	}
	
	/**
	 * Find the attribute set with the given name. Case insensitive
	 * check. 
	 * @param setName
	 * @return null if attribute set doesn't exist
	 */
	public AttributeSet findAttributeSet(String setName) {
		for (AttributeSet s : attributeSets) {
			if (s.getName().equals(setName)) return s;
		}
		return null;
	}
	

	/**
	 * 
	 * @return the feature metadata for the all data view
	 */
	public FeatureViewMetadata getAllViewMetadata() {
		return this.allViewMetadata;
	}
	
	/**
	 * the maximum version associated with any feature type 
	 * 
	 * @return
	 */
	public String getMaxVersion() {
		String version = types.get(0).getDataVersion();
		for (FeatureType t : types) {
			if (t.getDataVersion().compareTo(version) > 0) {
				version = t.getDataVersion();
			}
		}
		return version;
	}
	/**
	 * Initializes the feature type cache from the database
	 * on startup.
	 */
	@PostConstruct
    public void init() {
		
	    LoggerFactory.getLogger(getClass()).info("Loading Feature Type Metadata");

        this.setFeatureTypes( typeDao.getFeatureTypes() );      
        this.attributeSets = typeDao.getAttributeSets();
        this.attributeSets.add(new AttributeSet(AttributeSet.VECTOR_TILE, "include_vector_tile"));
        
        for (FeatureType t : types) {
        	LoggerFactory.getLogger(getClass()).info("Loading Feature Type Metadata ... " + t.getType());	
        	FeatureViewMetadata metadata = typeDao.getViewMetadata(t.getDataViewName(), this.attributeSets);
        	validateMetadata(t, metadata);
        	t.setViewMetadata( metadata );
        }
        
        allViewMetadata = typeDao.getViewMetadata(FeatureViewMetadata.ALL_FEATURES_VIEW_ROOT, this.attributeSets);
        validateMetadata(null, allViewMetadata);
        
        LoggerFactory.getLogger(getClass()).info("Loading Feature Type Metadata ... Complete");
    }

	/*
	 * validates the feature type metadata
	 */
	private void validateMetadata(FeatureType type, FeatureViewMetadata metadata) {
		//we need at least one geometry column 
		//and one column called cabd_id
		boolean hasid = false;
		boolean hasgeom = false;
		
		
		for (FeatureViewMetadataField field : metadata.getFields()) {
			if (field.getFieldName().equalsIgnoreCase(FeatureDao.ID_FIELD)) hasid = true;
			if (field.isGeometry()) {
				if (hasgeom) {
					throw new InvalidDatabaseConfigException(MessageFormat.format("The view ''{0}'' should only contain one geometry column", metadata.getFeatureView()));
				}
				hasgeom = true;
			}
		}
		
		if (type != null) {
			if (!hasid) throw new InvalidDatabaseConfigException(MessageFormat.format("The feature type ''{0}'' linked to the view ''{1}'' has no {2} column.  This column is required and should be configured in the database table {3}.", type.getType(), metadata.getFeatureView(), FeatureDao.ID_FIELD, FeatureTypeDao.FEATURE_METADATA_TABLE));
			if (!hasgeom) throw new InvalidDatabaseConfigException(MessageFormat.format("The feature type ''{0}'' linked to the view ''{1}'' has no geometry column field.  This column is required and should be configured in the database table {1}.", type.getType(), metadata.getFeatureView(), FeatureTypeDao.FEATURE_METADATA_TABLE));
		}else {
			if (!hasid) throw new InvalidDatabaseConfigException(MessageFormat.format("The view ''{0}'' has no {1} column.  This column is required and should be configured in the database table {2}.", FeatureViewMetadata.getAllFeaturesView(), FeatureDao.ID_FIELD, FeatureTypeDao.FEATURE_METADATA_TABLE));
			if (!hasgeom) throw new InvalidDatabaseConfigException(MessageFormat.format("The view ''{0}'' has no geometry column field.  This column is required and should be configured in the database table {1}.", FeatureViewMetadata.getAllFeaturesView(), FeatureTypeDao.FEATURE_METADATA_TABLE));
		}
	}
	
	/**
	 * Compute the data metadata associated with a feature type (min, max values etc). This is not
	 * static and will change as the data changes as a result it cannot be cached like other
	 * metadata.
	 * 
	 * @param type feature type to compute data metadata for
	 * @return
	 */
	public Map<FeatureViewMetadataField, FeatureViewMetadataFieldData> computeDataMetadata(FeatureType type) {
		return typeDao.computeDataMetadata(type);
	}
}
