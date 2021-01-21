package org.refractions.cabd.exceptions;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.springframework.beans.TypeMismatchException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.NoHandlerFoundException;


@ControllerAdvice
public class ExceptionMapper {
	
	@ExceptionHandler(InvalidParameterException.class)
	public ResponseEntity<ApiError> handleInvalidParameterException(
			InvalidParameterException ipe) {
		return new ResponseEntity<ApiError>(ipe.getError(), HttpStatus.BAD_REQUEST);
	}
	
	@ExceptionHandler(NotFoundException.class)
	public ResponseEntity<ApiError> handleNotFoundException(
			NotFoundException nfe) {
		return new ResponseEntity<ApiError>(nfe.getError(), HttpStatus.NOT_FOUND);
	}
	
	@ExceptionHandler(InvalidDatabaseConfigException.class)
	public ResponseEntity<ApiError> handleNotFoundException(
			InvalidDatabaseConfigException nfe) {
		return new ResponseEntity<ApiError>(nfe.getError(), HttpStatus.NOT_FOUND);
	}
	
	
	@ExceptionHandler(RuntimeException.class)
	public ResponseEntity<ApiError> handleRuntimeException(RuntimeException re) {
		StringWriter sw = new StringWriter();
		re.printStackTrace(new PrintWriter(sw));
		String exceptionAsString = sw.toString();
		String message = "Unexpected internal problem: " + re.getMessage()
				+ "\n" + exceptionAsString;
		return new ResponseEntity<ApiError>(new ApiError(message),
				HttpStatus.INTERNAL_SERVER_ERROR);
	}
	

	@ExceptionHandler(TypeMismatchException.class)
	public ResponseEntity<ApiError> handleTypeMismatchException(TypeMismatchException tme) {
		String message = "Error in parameter " + tme.getPropertyName() + ": " + tme.getMessage();
		return new ResponseEntity<ApiError>(new ApiError(message), HttpStatus.BAD_REQUEST);
	}
	
	@ExceptionHandler(NoHandlerFoundException.class)
	public ResponseEntity<String> handleNoHandlerFoundException(NoHandlerFoundException ex) {
		return new ResponseEntity<String>(HttpStatus.NOT_FOUND);
	}
}
