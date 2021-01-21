package org.refractions.cabd;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication()
@ComponentScan(basePackages = {"org.refractions.cabd"})
public class CabdApplication extends SpringBootServletInitializer {

	public static void main(String[] args) {
		SpringApplication.run(CabdApplication.class, args);
	}

}
