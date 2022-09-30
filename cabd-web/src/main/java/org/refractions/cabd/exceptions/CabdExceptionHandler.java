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

import java.text.MessageFormat;

import org.refractions.cabd.CabdConfigurationProperties;
import org.refractions.cabd.controllers.TooManyFeaturesException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
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
	
	@ExceptionHandler(InvalidParameterException.class)
	public ResponseEntity<ApiError> handleInvalidParameterException(
			InvalidParameterException ipe) {
		return new ResponseEntity<ApiError>(ipe.getError(), HttpStatus.BAD_REQUEST);
	}
	
	@ExceptionHandler(NotFoundException.class)
	public ResponseEntity<ApiError> handleNotFoundException(NotFoundException nfe) {
		return nfe.getError().toResponseEntity();
	}
	
	@ExceptionHandler(InvalidDatabaseConfigException.class)
	public ResponseEntity<ApiError> handleInvalidDatabaseConfiguration(
			InvalidDatabaseConfigException nfe) {
		return nfe.getError().toResponseEntity();
	}
	
    @ExceptionHandler(TooManyFeaturesException.class)
    public ResponseEntity<Object> handleExceptions( TooManyFeaturesException exception, WebRequest request) {
    	HttpHeaders headers = new HttpHeaders();
    	headers.setContentType(MediaType.APPLICATION_JSON);
    	
    	String message = MessageFormat.format("The results would return more than the maximum allowable features of {0}. Limit your request by adding a query filter to reduce the number of features or providing a custom max-results value less than {1}.", properties.getMaxresults(), properties.getMaxresults());
    	return handleExceptionInternal(exception, new ApiError(message, HttpStatus.FORBIDDEN), headers, HttpStatus.FORBIDDEN, request);
    }
    
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Object> handleExceptions( RuntimeException exception, WebRequest request) {
    	logger.error(exception.getMessage(), exception);
    	
    	HttpHeaders headers = new HttpHeaders();
    	headers.setContentType(MediaType.APPLICATION_JSON);
    	return handleExceptionInternal(exception, new ApiError(exception.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR), headers, HttpStatus.INTERNAL_SERVER_ERROR, request);
    }

}
