package org.refractions.cabd.controllers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.refractions.cabd.CabdConfigurationProperties;
import org.refractions.cabd.dao.CommunityDataDao;
import org.refractions.cabd.dao.FeatureDao;
import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.model.CommunityContact;
import org.refractions.cabd.model.CommunityData;
import org.refractions.cabd.model.CommunityData.Status;
import org.refractions.cabd.model.CommunityFeature;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Base64Utils;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

@Component
public class CommunityProcessor {

	private Logger logger = LoggerFactory.getLogger(CommunityProcessor.class);
	
	private AtomicBoolean isRunning = new AtomicBoolean(false);
	
	private BlobContainerClient blobContainerClient ;
	
	@Autowired
	private CommunityDataDao communityDao;

	@Autowired
	private FeatureDao featureDao;
	
	@Autowired
	private FeatureTypeManager typeManager;
	
	@Autowired
	CabdConfigurationProperties properties;
	
	
	@Async
	public void start() {
		if (isRunning.getAndSet(true)) return;
		try {
			logger.debug("Starting community data processing.");
			runInternal();
			logger.debug("Community data processing finished.");
		}finally {
			isRunning.set(false);
		}
		
	}
	
	private void runInternal() {
		try {
			initAzure();
		}catch (Throwable t) {
			logger.error("Cannot process features - unable to initialize Azure connection: " + t.getMessage(), t);
			return;
		}
		
		while(true) {
			
			CommunityData cd = communityDao.checkOutNext();
			if (cd == null) return;
			
			try {
				try {
					//begin transaction
					processData(cd);
				}catch (Exception ex) {
					logger.error("Unable to process raw community data: " + ex.getMessage(), ex);					
					cd.setStatus(Status.DONE_WARN);
					cd.getWarnings().add("ERROR: " + ex.getMessage());
					communityDao.updateStatus(cd);				
				}
			}catch (Throwable t) {
				logger.error("Unable to process raw community data: " + t.getMessage(), t);
			}
		}
	}
	
	private void initAzure() {
		
		DefaultAzureCredential defaultCredential = new DefaultAzureCredentialBuilder()
				.build();
		
		// Azure SDK client builders accept the credential as a parameter
		BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
		        .endpoint(properties.getAzureStorageaccounturl())
		        .credential(defaultCredential)
		        .buildClient();
		
		// Create a unique name for the container
		String containerName = properties.getAzureContainername();

		// get blob container
		blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
	}
	
	@Transactional
	private void processData(CommunityData data) {
		
		data.setWarnings(new ArrayList<>());
		
		Pair<List<CommunityFeature>, List<String>> parsedresults = parseJon(data.getData());
		
		List<CommunityFeature> features = parsedresults.getLeft();
		
		data.getWarnings().addAll(parsedresults.getRight());
		
		for (CommunityFeature feature : features) {
			
			feature.setRaw(data);
			
			FeatureType featuretype = typeManager.getFeatureType(feature.getFeatureType());
			if (featuretype == null) {
				data.getWarnings().add(MessageFormat.format("Feature {0): Feature type {1} not found. Community data not processed.", feature.getIndex(), feature.getFeatureType()));
				continue;
			}
			
			
			if (feature.getCabdId() == null) {
				//new feature create a new cabd_id
				feature.setCabdId(UUID.randomUUID());
			}else {
				//validate feature exists in feature type
				Feature f = featureDao.getFeature(feature.getCabdId());
				if (f == null) {
					//feature doesn't exist; not sure where uuid came from but I guess it doesn't matter
					//let's generate a new id & a warning
					data.getWarnings().add(MessageFormat.format("Feature {0}: Feature with id {0} not found in cabd. New cabd_id will be generated and the feature will be processed.  ",feature.getIndex(), feature.getCabdId().toString()));
					feature.setCabdId(UUID.randomUUID());	
				}else {
					if (!f.getFeatureType().equalsIgnoreCase(featuretype.getType())) {
						data.getWarnings().add(MessageFormat.format("Feature {0}: Inconsistent feature types. Feature type associated with id {0} in the database is {1}, but json feature type is {2}. Community data not processed.",feature.getIndex(), featuretype.getType(), f.getFeatureType()));
						continue;
					}
				}	
			}
			
			if (featuretype.getCommunityDataTable() == null) {
				data.getWarnings().add(MessageFormat.format("Feature {0}: Community data table not specified for feature type {1}. Community data not processed.", feature.getIndex(), feature.getFeatureType()));
				continue;
			}
			
			//parse user name
			try {
				feature.setCommunityContact(parseUser(feature.getUsername()));
			}catch (Exception ex) {
				logger.warn(ex.getMessage(), ex);
				data.getWarnings().add(MessageFormat.format("Feature {0}: Could not create community user for feature. username: {1} error: {2}. Community data not processed.", feature.getIndex(), feature.getUsername(), ex.toString()));
				continue;
			}
			
			JsonObject properties = feature.getJson().get("properties").getAsJsonObject();

			String fileIdPrefix = feature.getId().toString().replaceAll("-", "");
			//parse photos
			try {
			for (String field : featuretype.getCommunityPhotoFields()) {
				JsonElement photodata = properties.get(field);
				if (photodata == null) continue;
				if (photodata.getAsString() == null || photodata.getAsString().isBlank()) continue;

				//find the photo field in the json properties
				String fileId = fileIdPrefix + "_" + field + ".jpeg";
				try {
					
					String base64 = properties.get(field).getAsString();
					byte[] imagedata = Base64Utils.decodeFromString(base64);
					
					writeToAzure(fileId, imagedata);
					
					properties.remove(field);
					properties.add(field, new JsonPrimitive(fileId));
					
				}catch (Exception ex) {
					logger.warn(ex.getMessage(), ex);
					data.getWarnings().add(MessageFormat.format("Feature {0}: Could not write image data for photo field {1} to azure: {2}. Community data not processed.", feature.getIndex(), field, ex.toString()));	
					throw ex;
				}
				
			}
			}catch (Exception ex) {
				continue;
			}
			
			//save to feature type table
			try {
				communityDao.saveCommunityFeature(featuretype.getCommunityDataTable(), feature);
			}catch (Exception ex) {
				data.getWarnings().add(MessageFormat.format("Feature {0}: Could not save feature to the feature staging table. Community data not processed. PHOTO files were uploaded to Azure so these need to be removed manually, files that start with id '{1}'.", feature.getIndex(), fileIdPrefix));
			}
		}
		
		if (data.getWarnings().isEmpty()) {
			//delete this row otherwise we will be duplicating data 
			data.setStatus(Status.DONE);
			communityDao.deleteRawData(data);
		}else {
			data.setStatus(Status.DONE_WARN);
			data.setStatusMessage("Processing completed with warnings/errors.");
			communityDao.updateStatus(data);
		}
		
	}
	
	private void writeToAzure(String filename, byte[] imagedata) throws Exception{
		BlobClient blobClient = blobContainerClient.getBlobClient(filename);
		
		logger.debug(MessageFormat.format("uploading {0} to azure ", filename));

		// Upload the blob
		Path temp = Files.createTempFile("cwf_mobile", null);
		try {
			Files.write(temp, imagedata);
			blobClient.uploadFromFile(temp.toAbsolutePath().toString());
			
		}catch (Exception ex) {
			throw ex;
		}finally {
			try {
				Files.delete(temp);
			}catch (Exception ex) {
				logger.warn(ex.getMessage(), ex);
			}
		}
		
		
	}
	
	private CommunityContact parseUser(String username) {
		return communityDao.getOrCreateCommunityContact(username);
	}
	
	private static Pair<List<CommunityFeature>, List<String>> parseJon(String json) {
		JsonElement root = JsonParser.parseString(json);
		
		List<CommunityFeature> features = new ArrayList<>();
		List<String> warnings = new ArrayList<>();
		if (root.isJsonArray()) {
			JsonArray all = (JsonArray)root;
			for (int i = 0; i < all.size(); i ++) {
				try{
					CommunityFeature cd = processFeature( all.get(i) );
					cd.setIndex(i+1);
					features.add(cd);
				}catch (Exception ex) {
					warnings.add(MessageFormat.format("Could not parse feature number {0} in file: {1}", i, ex.getMessage()));
				}
			}
		}else if (root.isJsonObject()) {
			try{
				CommunityFeature cd = processFeature( root );
				cd.setIndex(1);
				features.add(cd);
			}catch (Exception ex) {
				warnings.add(MessageFormat.format("Could not parse feature {0}.", ex.getMessage()));
			}
		}
		
		return Pair.of(features, warnings);
	}
	
	private static CommunityFeature processFeature(JsonElement feature) throws Exception {
		if (!feature.isJsonObject()) {
			throw new Exception("Not a json object");
		}
		
		JsonObject j = feature.getAsJsonObject();
		checkAttributes(j, "type", "geometry", "properties");
		
		if (!j.get("type").getAsString().equalsIgnoreCase("feature")) {
			throw new Exception("Invalid GeoJson - type attribute not set to feature");
		}
		
		//
		JsonElement geom = j.get("geometry");
		if (!geom.isJsonObject()) {
			throw new Exception("Invalid GeoJson - geometry attribute is invalid.");
		}
		
		JsonObject ggeom = geom.getAsJsonObject();
		checkAttributes(ggeom, "type", "coordinates");

		//Geometry g = parseGeometry(ggeom.get("type").getAsString(), ggeom.get("coordinates"));
		
		JsonElement properties = j.get("properties");
		if (!properties.isJsonObject()) {
			throw new Exception("Invalid GeoJson - properties attribute is invalid.");
		}
		checkAttributes(properties.getAsJsonObject(), "feature_type", "user_email");
		
		String featureType = properties.getAsJsonObject().get("feature_type").getAsString();
		String useremail = properties.getAsJsonObject().get("user_email").getAsString();
		UUID cabdId = null;
		if (properties.getAsJsonObject().has("cabd_id")) {
			String cid = properties.getAsJsonObject().get("cabd_id").getAsString();
			if (!cid.trim().isBlank()) {
				try {
					cabdId = UUID.fromString(cid);
				}catch (Exception ex) {
					throw new Exception("Invalid cabdId - '" + cid + "' is not a valid uuid.");
				}
			}
		}
		return new CommunityFeature(cabdId, featureType, useremail, j);
		
	}
	
//	private static Geometry parseGeometry(String type, JsonElement coordinates) {
//		GeometryFactory gf = new GeometryFactory();
//		
//		if (type.equalsIgnoreCase("POINT")) {
//			JsonArray c = coordinates.getAsJsonArray();
//			Point pnt = gf.createPoint(new Coordinate(c.get(0).getAsDouble(), c.get(1).getAsDouble()));
//			return pnt;
//		}
//		System.out.println("feature type not supported");
//		return null;
//		
//	}
	
	private static void checkAttributes(JsonObject o, String...attributes) throws Exception {
		for (String r : attributes) {
			if (!o.has(r)) {
				throw new Exception("Invalid GeoJson - " + r + " attribute not found");
			}
		}
	}
	
}
