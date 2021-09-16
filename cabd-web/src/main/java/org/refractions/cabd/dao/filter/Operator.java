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

import org.refractions.cabd.exceptions.InvalidParameterException;

/**
 * Filter operator
 * 
 * @author Emily
 *
 */
public enum Operator {

	IN ("in"),
	NOTIN ("notin"),
	EQ ("eq"),
	NEQ("neq"),
	LT("lt"),
	LTE("lte"),
	GT("gt"),
	GTE("gte"),
	LIKE("like");
	
	String key;
	
	Operator(String key){
		this.key = key;
	}
	
	public String getKey() {
		return this.key;
	}
	
	public String toSql() {
		switch(this) {
		case EQ: return "=";
		case GT: return ">";
		case GTE: return ">=";
		case IN: return " IN ";
		case LIKE: return " ILIKE ";
		case LT: return "<";
		case LTE: return "<=";
		case NEQ: return "!=";
		case NOTIN: return "NOT IN";		
		};
		return "";
	}
	
	public boolean supports(DataType dataType) {
		switch(dataType) {
		case STRING: return this == IN ||  this == NOTIN || this == EQ || this == NEQ || this == LIKE;
		case NUMBER: 
		case DATE: return this == IN || this == NOTIN || this == EQ || this == NEQ || this == LT || this == LTE || this == GT || this == GTE;
		case BOOLEAN: return this == EQ || this == NEQ;
		case UUID: return this == EQ ;
		}
		return false;
	}
	
	public static Operator findOperator(String op) {
		for (Operator o : Operator.values()) {
			if (o.key.equalsIgnoreCase(op)) return o;
		}
		throw new InvalidParameterException("Operator not found: " + op);
	}
}
