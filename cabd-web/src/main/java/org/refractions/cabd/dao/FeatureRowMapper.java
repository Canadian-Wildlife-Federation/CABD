package org.refractions.cabd.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;
import org.springframework.jdbc.core.RowMapper;

/**
 * Maps the results of a feature query to 
 * a feature object.
 * 
 * @author Emily
 *
 */
public class FeatureRowMapper implements RowMapper<Feature> {

	
	private FeatureViewMetadata metadata;
	
	private WKBReader reader = new WKBReader();
	
	public FeatureRowMapper(FeatureViewMetadata metadata) {
		this.metadata = metadata;
	}
	
	@Override
	public Feature mapRow(ResultSet rs, int rowNum) throws SQLException {
		UUID buuid = (UUID) rs.getObject(FeatureDao.ID_FIELD);
		Feature feature = new Feature(buuid);
		
		for (FeatureViewMetadataField field : metadata.getFields()) {
			if (field.isGeometry()) {
				try {
					byte[] data = rs.getBytes(field.getFieldName());
					if (data != null) {
						Geometry geom = reader.read(data);
						feature.setGeometry(geom);
					}
				}catch (Exception ex) {
					throw new SQLException(ex);
				}
			}else if (field.isLink()) {
				UUID oo = (UUID)rs.getObject(field.getFieldName());
				feature.addLinkAttribute(field.getName(), oo);
			}else {
				Object oo = rs.getObject(field.getFieldName());
				feature.addAttribute(field.getName(), oo);
			}
		};
		return feature;
	}

}
