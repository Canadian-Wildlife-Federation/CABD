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
package org.refractions.cabd.exceptions;

import java.time.LocalDateTime;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Generic error field contains a date/time and message.
 * @author Emily
 *
 */
public class ApiError {

	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy hh:mm:ss")
	private LocalDateTime timestamp;
	private String message;
	private HttpStatus status;
	
	private ApiError() {
		timestamp = LocalDateTime.now();
		this.message = "";
		this.status = HttpStatus.INTERNAL_SERVER_ERROR;
	}

	public ApiError(String message, HttpStatus status) {
		this();
		this.message = message;
		this.status = status;
	}
	
	public ApiError(Throwable ex, HttpStatus status) {
		this(ex.getMessage(), status);
	}

	
	public ResponseEntity<ApiError> toResponseEntity(){
		return new ResponseEntity<ApiError>(this, this.status);
	}
	
	public int getStatusCode() {
		return this.status.value();
	}
	
	public String getStatusMessage() {
		return this.status.getReasonPhrase();
	}
	
	public String getMessage() {
		return this.message;
	}

}
