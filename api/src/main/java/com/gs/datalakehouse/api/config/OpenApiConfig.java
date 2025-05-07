package com.gs.datalakehouse.api.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Configuration for OpenAPI documentation.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI dataLakehouseOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Data Lakehouse API")
                        .description("REST API for the Goldman Sachs Data Lakehouse Service")
                        .version("1.0.0")
                        .contact(new Contact()
                                .name("Goldman Sachs Innovation Center")
                                .url("https://github.com/goldmansachsinnovationcenter")
                                .email("innovation@gs.com"))
                        .license(new License()
                                .name("Proprietary")
                                .url("https://www.goldmansachs.com")))
                .servers(List.of(
                        new Server()
                                .url("/")
                                .description("Default Server URL")
                ));
    }
}
