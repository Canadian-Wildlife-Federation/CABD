package org.refractions.cabd.model;

import org.refractions.cabd.CabdApplication;

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
