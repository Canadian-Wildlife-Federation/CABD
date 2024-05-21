package org.refractions.cabd.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Maps to raw (unparsed) community data request.
 * 
 * @author Emily
 *
 */
public class CommunityData {

	private UUID id;
	private String data;
	private Instant uploadeddatetime; 
	
	private Status status;
	private String statusMessage;
	private List<String> warnings;
	
	public enum Status{
		NEW, 
		PROCESSING, 
		DONE, 
		DONE_WARN
	}
	
	public CommunityData(String data, Instant datetime) {
		this(null, data, datetime);
	}
	
	public CommunityData(UUID id, String data, Instant datetime) {
		this.id = id;
		this.data = data;
		this.uploadeddatetime = datetime;
		
	}
	
	public CommunityData(UUID id, String data, Instant datetime, String status) {
		this.id = id;
		this.data = data;
		this.uploadeddatetime = datetime;
		this.status = Status.valueOf(status);
		this.data = data;
		
	}
	
	public void setStatus(Status s) {
		this.status = s;
	}
	public Status getStatus() {
		return this.status;
	}
	public void setStatusMessage(String message) {
		this.statusMessage = message;
	}
	public String getStatusMessage() {
		return this.statusMessage;
	}
	public void setWarnings(List<String> warnings) {
		this.warnings = warnings;
	}
	
	public List<String> getWarnings(){
		return this.warnings;
	}
	public String[] getWarningsArray() {
		return this.warnings.toArray(new String[this.warnings.size()]);
	}
	public Instant getUploadeddatetime() {
		return uploadeddatetime;
	}

	public void setUploadeddatetime(Instant uploadeddatetime) {
		this.uploadeddatetime = uploadeddatetime;
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}
	
	
}
