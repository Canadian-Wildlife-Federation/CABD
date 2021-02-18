package org.refractions.cabd;

import javax.sql.DataSource;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication()
@ComponentScan(basePackages = {"org.refractions.cabd"})
public class CabdApplication extends SpringBootServletInitializer {

	//database connection variables
	private static final String DB_URL_ENV = "CABD_DB_URL";
	private static final String DB_USER_ENV = "CABD_DB_USER";
	private static final String DB_PASS_ENV = "CABD_DB_PASSWORD";
	
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
}
