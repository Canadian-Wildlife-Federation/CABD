package org.refractions.cabd.dao;

import java.util.UUID;

import org.refractions.cabd.exceptions.NotFoundException;
import org.refractions.cabd.model.Contact;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureChangeRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class UserFeatureUpdateDao {

	private static final String TABLE = "cabd.user_feature_updates";

	public enum Status{
		NEEDS_REVIEW,
		DONE
	};
	
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	private ContactDao contactDao;
	
	@Autowired
	private FeatureDao featureDao;
	
	
	/**
	 * Adds a new record to the feature update tables;
	 * Add new contact and/or updates contact information
	 *   
	 */
	@Transactional
	public void newFeatureUpdate(UUID cabdId, FeatureChangeRequest changeRequest) {

		Feature cabdFeature = featureDao.getFeature(cabdId);
		if (cabdFeature == null)
			throw new NotFoundException("Feature not found");

		Contact c = contactDao.getUpdateOrCreateContact(changeRequest.getEmail(), 
				changeRequest.getName(), changeRequest.getOrganization(), changeRequest.getMailinglist());
		
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(TABLE);
		sb.append(" (contact_id, cabd_id, cabd_type, user_description, user_data_source, status)");
		sb.append(" VALUES (?, ?, ?, ?, ?, ?)");

		jdbcTemplate.update(sb.toString(),
				c.getId(), cabdFeature.getId(), cabdFeature.getFeatureType(), changeRequest.getDescription(), changeRequest.getDatasource(),
						Status.NEEDS_REVIEW.name().toLowerCase() );

	}
				
}
