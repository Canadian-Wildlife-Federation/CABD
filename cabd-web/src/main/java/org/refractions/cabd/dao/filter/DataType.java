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
package org.refractions.cabd.dao.filter;

import java.security.InvalidParameterException;
import java.text.MessageFormat;

/**
 * Data field type; used for parsing filters
 * 
 * @author Emily
 *
 */
public enum DataType{
	STRING,
	NUMBER,
	BOOLEAN,
	DATE,
	UUID;
	
	public static DataType findDataType(String dataType) {
		if (dataType.toLowerCase().startsWith("varchar")) return DataType.STRING;
		if (dataType.toLowerCase().startsWith("text")) return DataType.STRING;
		if (dataType.toLowerCase().startsWith("boolean")) return DataType.BOOLEAN;
		if (dataType.toLowerCase().startsWith("integer")) return DataType.NUMBER;
		if (dataType.toLowerCase().startsWith("double")) return DataType.NUMBER;
		if (dataType.toLowerCase().startsWith("uuid")) return DataType.UUID;
		if (dataType.toLowerCase().startsWith("date")) return DataType.DATE;
		//TODO: sort out array data type
		//if (dataType.toLowerCase().startsWith("array")) return LocalDate.class;
		throw new InvalidParameterException(MessageFormat.format("Filtering on data type {0} not supported.", dataType));
	}
}