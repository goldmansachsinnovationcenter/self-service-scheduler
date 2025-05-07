package com.gs.datalakehouse.core.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Configuration for file system services.
 */
@Configuration
@PropertySource("classpath:config/application.yml")
public class FileSystemConfig {
}
