package com.example.insertdata.config;


import org.apache.camel.spring.spi.SpringTransactionPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class TransactionConfig {

    @Bean
    public DataSourceTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = "PROPAGATION_REQUIRED")
    public SpringTransactionPolicy propagationRequiredPolicy(DataSourceTransactionManager transactionManager) {
        SpringTransactionPolicy policy = new SpringTransactionPolicy();
        policy.setTransactionManager(transactionManager);
        policy.setPropagationBehaviorName("PROPAGATION_REQUIRED");
        return policy;
    }
}