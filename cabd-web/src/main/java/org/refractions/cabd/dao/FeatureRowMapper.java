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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.refractions.cabd.controllers.AttributeSet;
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
	private AttributeSet attributes;
	
	private WKBReader reader = new WKBReader();
	
	public FeatureRowMapper(FeatureViewMetadata metadata, AttributeSet attributes) {
		this.metadata = metadata;
		this.attributes = attributes;
	}
	
	@Override
	public Feature mapRow(ResultSet rs, int rowNum) throws SQLException {
		UUID buuid = (UUID) rs.getObject(FeatureDao.ID_FIELD);
		String featureType = (String)rs.getString(FeatureDao.FEATURE_TYPE_FIELD);
		
		Feature feature = new Feature(buuid, featureType);
		
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
			}else {
				if (attributes == AttributeSet.ALL || field.includeVectorTile()) {
					if (field.isLink()) {
						String oo = (String)rs.getObject(field.getFieldName());
						feature.addLinkAttribute(field.getFieldName(), oo);
					}else {
						Object oo = rs.getObject(field.getFieldName());
						feature.addAttribute(field.getFieldName(), oo);
					}
				}
			}
		};
		return feature;
	}

}
