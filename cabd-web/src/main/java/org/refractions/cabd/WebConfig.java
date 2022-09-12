/*
 * Copyright 2019 Government of Canada
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
package org.refractions.cabd;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {
	
    @Override
	public void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
		configurer
				.favorParameter(true)
				.parameterName("format")
				//.ignoreAcceptHeader(true)
//				.defaultContentType(CabdApplication.GEOJSON_MEDIA_TYPE)
				.mediaType("json", MediaType.APPLICATION_JSON)
				.mediaType("geojson", CabdApplication.GEOJSON_MEDIA_TYPE)
				.mediaType("geopackage", CabdApplication.GEOPKG_MEDIA_TYPE)
				.mediaType("gpkg", CabdApplication.GEOPKG_MEDIA_TYPE)
				.mediaType("shp", CabdApplication.SHP_MEDIA_TYPE)
				.mediaType("kml", CabdApplication.KML_MEDIA_TYPE)
				.mediaType("csv", CabdApplication.CSV_MEDIA_TYPE)
				
			;
	}
	
	@Override
	public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
        .allowedOrigins("*")
        .allowedMethods("GET","POST","PUT","DELETE");
    }
	
}
