/*
 * Copyright 2022 Canadian Wildlife Federation
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

import org.refractions.cabd.CabdApplication;

/**
 * Extension of named item that adds support for english and french description fields.
 * The returned description is based on the request locale
 * 
 * @author Emily
 *
 */
public class NamedDescriptionItem extends NamedItem {
	
	protected String description_en;
	protected String description_fr;
	
	public NamedDescriptionItem(String name_en, String name_fr,
			String description_en, String description_fr) {
		super(name_en, name_fr);
		this.description_en = description_en;
		this.description_fr = description_fr;
	}
	
	
	public String getDescription() {
		if (CabdApplication.isFrench())  return description_fr;
		return description_en;	
	}
}
