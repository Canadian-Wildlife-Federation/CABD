package org.refractions.cadb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication()
@ComponentScan(basePackages = {"org.refractions.cadb"})
public class CadbApplication extends SpringBootServletInitializer {

	public static void main(String[] args) {
		SpringApplication.run(CadbApplication.class, args);
	}

}
