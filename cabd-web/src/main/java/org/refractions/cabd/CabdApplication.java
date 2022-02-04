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
package org.refractions.cabd;

import java.nio.charset.Charset;

import javax.sql.DataSource;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.MediaType;

@SpringBootApplication()
@ComponentScan(basePackages = {"org.refractions.cabd"})
public class CabdApplication extends SpringBootServletInitializer {

	//database connection variables
	private static final String DB_URL_ENV = "CABD_DB_URL";
	private static final String DB_USER_ENV = "CABD_DB_USER";
	private static final String DB_PASS_ENV = "CABD_DB_PASSWORD";
	
	public static final String GEOPKG_MEDIA_TYPE_STR = "application/geopackage+sqlite3";
	public static final MediaType GEOPKG_MEDIA_TYPE = new MediaType("application", "geopackage+sqlite3", Charset.forName("UTF-8"));
	
	public static final MediaType GEOJSON_MEDIA_TYPE = new MediaType("application", "geo+json",Charset.forName("UTF-8"));
	
	public static final String SHP_MEDIA_TYPE_STR = "application/shapefile";
	public static final MediaType SHP_MEDIA_TYPE = new MediaType("application", "shapefile");
	
	public static final String KML_MEDIA_TYPE_STR = "application/vnd.google-earth.kml+xml";
	public static final MediaType KML_MEDIA_TYPE = new MediaType("application", "vnd.google-earth.kml+xml");
			
	public static final String CSV_MEDIA_TYPE_STR = "text/csv";
	public static final MediaType CSV_MEDIA_TYPE = new MediaType("text", "csv");
		
	public static void main(String[] args) {
		SpringApplication.run(CabdApplication.class, args);
	}

	
    @Bean
    public DataSource getDataSource() 
    {
    	String url = System.getProperty(DB_URL_ENV);
    	if (url == null) url = System.getenv(DB_URL_ENV);
    	
    	String user = System.getProperty(DB_USER_ENV);
    	if (user == null) user = System.getenv(DB_USER_ENV);
    	
    	String password = System.getProperty(DB_PASS_ENV);
    	if (password == null) password= System.getenv(DB_PASS_ENV);
    	
    	if (url == null || user == null || password == null) {
    		throw new IllegalStateException("The db connection environment variables (" + DB_URL_ENV + ", " + DB_USER_ENV + ", " + DB_PASS_ENV + ") are not configured.");
    	}
    	
        DataSourceBuilder<?> dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName("org.postgresql.Driver");
        dataSourceBuilder.url(url);
        dataSourceBuilder.username(user);
        dataSourceBuilder.password(password);
        return dataSourceBuilder.build();
    }
    
    
//    @Bean
//    public HttpMessageConverters customConverters() {
//    	FeatureListGeoPkgSerializer geopkgConverter = new FeatureListGeoPkgSerializer();
////    	FeatureListJsonSerializer jsonConverter = new FeatureListJsonSerializer();
//    	return new HttpMessageConverters(geopkgConverter);
//    }
}
