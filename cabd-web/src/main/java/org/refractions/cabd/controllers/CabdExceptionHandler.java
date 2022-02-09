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
package org.refractions.cabd.controllers;

import java.text.MessageFormat;

import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.CabdConfigurationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * CABD Exception handler
 * 
 * @author Emily
 *
 */
@ControllerAdvice
public class CabdExceptionHandler extends ResponseEntityExceptionHandler {

	private Logger logger = LoggerFactory.getLogger(CabdExceptionHandler.class);
	
	@Autowired
	private CabdConfigurationProperties properties;
	
    @ExceptionHandler(TooManyFeaturesException.class)
    public ResponseEntity<Object> handleExceptions( TooManyFeaturesException exception, WebRequest request) {
    	HttpHeaders headers = new HttpHeaders();
    	headers.setContentType(MediaType.APPLICATION_JSON);
    	
    	String message = MessageFormat.format("The results would return more than the maximum allowable features of {0}. Limit your request by adding a query filter to reduce the number of features or providing a custom max-results value less than {1}.", properties.getMaxresults(), properties.getMaxresults());
    	return handleExceptionInternal(exception, new CabdError(message, HttpStatus.FORBIDDEN), headers, HttpStatus.FORBIDDEN, request);
    }
    
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Object> handleExceptions( RuntimeException exception, WebRequest request) {
    	logger.error(exception.getMessage(), exception);
    	
    	HttpHeaders headers = new HttpHeaders();
    	headers.setContentType(MediaType.APPLICATION_JSON);
    	return handleExceptionInternal(exception, new CabdError(exception.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR), headers, HttpStatus.INTERNAL_SERVER_ERROR, request);
    }
    
    /*
     * Error class for convert to JSON respose
     */
    private class CabdError{
    	String message;
    	HttpStatus status;
    	
    	public CabdError(String message, HttpStatus status) {
    		this.message = message;
    		this.status = status;
    	}
    	public String getMessage() {
    		return this.message;
    	}
    	public int getStatusCode() {
    		return this.status.value();
    	}
    	public String getStatusMessage() {
    		return this.status.getReasonPhrase();
    	}
    	
    }
}
