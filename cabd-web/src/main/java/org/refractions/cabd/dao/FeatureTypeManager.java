package org.refractions.cabd.dao;

import java.text.MessageFormat;
import java.util.List;

import javax.annotation.PostConstruct;

import org.refractions.cabd.exceptions.InvalidDatabaseConfigException;
import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;
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

	@Autowired
	private FeatureTypeDao typeDao;
	
	//cached types and metadata
	private List<FeatureType> types;
	private FeatureViewMetadata allViewMetadata;
	
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
	 * 
	 * @return the feature metadata for the all data view
	 */
	public FeatureViewMetadata getAllViewMetadata() {
		return this.allViewMetadata;
	}
	
	/**
	 * Initializes the feature type cache from the database
	 * on startup.
	 */
	@PostConstruct
    public void init() {
		
        this.setFeatureTypes( typeDao.getFeatureTypes() );      
        
        for (FeatureType t : types) {
        	FeatureViewMetadata metadata = typeDao.getViewMetadata(t.getDataView());
        	validateMetadata(t, metadata);
        	t.setViewMetadata( metadata );
        }
        
        allViewMetadata = typeDao.getViewMetadata(FeatureViewMetadata.ALL_FEATURES_VIEW);
        validateMetadata(null, allViewMetadata);
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
				hasgeom = true;
				if (field.getSRID() != FeatureDao.DATABASE_SRID){
					throw new InvalidDatabaseConfigException(MessageFormat.format("The view ''{0}'' should only contain one geometry column with the projection {1}", metadata.getFeatureView(), FeatureDao.DATABASE_SRID));
				}
			}
		}
		
		if (type != null) {
			if (!hasid) throw new InvalidDatabaseConfigException(MessageFormat.format("The feature type ''{0}'' linked to the view ''{1}'' has no {2} column.  This column is required and should be configured in the database table {3}.", type.getType(), metadata.getFeatureView(), FeatureDao.ID_FIELD, FeatureTypeDao.FEATURE_METADATA_TABLE));
			if (!hasgeom) throw new InvalidDatabaseConfigException(MessageFormat.format("The feature type ''{0}'' linked to the view ''{1}'' has no geometry column field.  This column is required and should be configured in the database table {1}.", type.getType(), metadata.getFeatureView(), FeatureTypeDao.FEATURE_METADATA_TABLE));
		}else {
			if (!hasid) throw new InvalidDatabaseConfigException(MessageFormat.format("The view ''{0}'' has no {1} column.  This column is required and should be configured in the database table {2}.", FeatureViewMetadata.ALL_FEATURES_VIEW, FeatureDao.ID_FIELD, FeatureTypeDao.FEATURE_METADATA_TABLE));
			if (!hasgeom) throw new InvalidDatabaseConfigException(MessageFormat.format("The view ''{0}'' has no geometry column field.  This column is required and should be configured in the database table {1}.", FeatureViewMetadata.ALL_FEATURES_VIEW, FeatureTypeDao.FEATURE_METADATA_TABLE));
		}
	}
}
