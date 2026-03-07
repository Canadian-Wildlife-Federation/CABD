package org.refractions.cabd;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoders;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Configures our application with Spring Security to restrict access to our API
 * endpoints.
 */
@Configuration
public class SecurityConfig {

	@Bean
	SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
		/*
		 *configure the security required for our endpoints and setup
		 * our app to serve as an OAuth2 Resource Server
		 * using JWT validation.
		 */
		return http
				.authorizeHttpRequests((authorize) -> authorize
						.requestMatchers("/community/**").authenticated()
						.anyRequest().permitAll()
						)
				.cors(Customizer.withDefaults())
				.oauth2ResourceServer(oauth2 -> oauth2.jwt(Customizer.withDefaults())).build();
		
		
		/*@GetMapping("/features/waterfalls")
		public List<Waterfall> getWaterfalls(@AuthenticationPrincipal Jwt jwt) {
		    String userId = jwt.getClaim("sub");      // Auth0 user ID
		    String email = jwt.getClaim("email");     // if available

		    return service.findByUser(userId);
		}
		*/

	}
	

	/**
	 * validate the correct audience api is being used
	 * 
	 * @param issuer
	 * @param audience
	 * @return
	 */
	@Bean
	JwtDecoder jwtDecoder(
	        @Value("${spring.security.oauth2.resourceserver.jwt.issuer-uri}") String issuer,
	        @Value("${auth0.audience}") String audience) {
	    NimbusJwtDecoder decoder = JwtDecoders.fromIssuerLocation(issuer);
	    OAuth2TokenValidator<Jwt> withIssuer = JwtValidators.createDefaultWithIssuer(issuer);
	    OAuth2TokenValidator<Jwt> withAudience = new AudienceValidator(audience);
	    decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(withIssuer, withAudience));
	    return decoder;
	}

}