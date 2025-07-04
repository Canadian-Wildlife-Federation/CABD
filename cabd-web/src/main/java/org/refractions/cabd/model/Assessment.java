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
package org.refractions.cabd.model;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Class for tracking basic assessment information (id, date, type)
 * 
 * @author Emily
 *
 */
public class Assessment {

	private UUID assessmentId;
	private LocalDateTime datetime;
	private String assessmentType;
	
	private String refUrl;
	
	public Assessment(UUID assessmentId, LocalDateTime datetime, String assessmentType) {
		super();
		this.assessmentId = assessmentId;
		this.datetime = datetime;
		this.assessmentType = assessmentType;
	}
	
	public UUID getAssessmentId() {
		return assessmentId;
	}
	
	public LocalDateTime getDatetime() {
		return datetime;
	}
	
	public String getAssessmentType() {
		return assessmentType;
	}

	public String getUrl() {
		return this.refUrl + "/" + assessmentId.toString();
	}
	
	public void setRefUrl(String url) {
		this.refUrl = url;
	}
	
}
