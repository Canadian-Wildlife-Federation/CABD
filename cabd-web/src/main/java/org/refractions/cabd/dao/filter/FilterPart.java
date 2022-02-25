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

import java.text.MessageFormat;
import java.time.LocalDate;
import java.util.UUID;

import org.refractions.cabd.exceptions.InvalidParameterException;

/**
 * Individual filter part
 * 
 * @author Emily
 *
 */
public class FilterPart {

	private String attributeKey;
	private Operator operator;
	private String[] values;
	
	private DataType dataType;
	
	public FilterPart(String attributeKey, Operator operator, String[] values) {
		this.attributeKey = attributeKey;
		this.operator = operator;
		this.values = values;
	}
	
	public FilterPart(String attributeKey, Operator operator, String value) {
		this.attributeKey = attributeKey;
		this.operator = operator;
		this.values = new String[] {value};
	}
	
	public String getAttributeKey() {
		return this.attributeKey;
	}
	
	public Operator getOperator() {
		return this.operator;
	}
	
	public String[] getValues() {
		return this.values;
	}
	
	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}
	
	public DataType getDataType() {
		return this.dataType;
	}
	
	public Object[] getValuesAsDataType()  {
		Object[] items = new Object[values.length];
		for (int i = 0; i < values.length; i ++) {
			items[i] = toDataType(values[i]);
		}
		return items;
	}
	
	private Object toDataType(String value) {
		switch(getDataType()) {
		case BOOLEAN:
			return Boolean.valueOf(value);
		case DATE:
			//ISO-8601 YYYY-MM-DD
			return LocalDate.parse(value);
		case NUMBER:
			return Double.valueOf(value);
		case STRING:
			return value;
		case UUID:
			if (value.trim().isBlank()) return null;
			return UUID.fromString(value);
		}
		throw new InvalidParameterException(MessageFormat.format("Could not parse {0} into data type {1}", value, getDataType().name()));
	}
}
