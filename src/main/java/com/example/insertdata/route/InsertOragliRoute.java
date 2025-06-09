package com.example.insertdata.route;


import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Component
public class InsertOragliRoute extends RouteBuilder {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

    public InsertOragliRoute(DataSource dataSource) {
        this.dataSource = dataSource;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    private Object convertToNumber(Object value) {
        if (value == null) return null;
        if (value instanceof String) {
            String str = ((String) value).trim();
            if (str.isEmpty()) return null;
            try {
                if (str.contains(".")) {
                    return Double.parseDouble(str);
                } else {
                    return Long.parseLong(str);
                }
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return value;
    }

    @Override
    public void configure() throws Exception {

        onException(Exception.class)
                .handled(true)
                .log("Error occurred: ${exception.stacktrace}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setBody(simple("{\"error\":\"${exception.message}\"}"))
                .rollback();

        restConfiguration()
                .component("servlet")
                .bindingMode(RestBindingMode.json)
                .dataFormatProperty("prettyPrint", "true")
                .contextPath("/api")
                .port(8080);

        rest("/oragli")
                .post("/batch")
                .consumes("application/json")
                .produces("application/json")
                .to("direct:processOragli");

        from("direct:processOragli")
                .routeId("oragli-insert-route")
                .transacted("PROPAGATION_REQUIRED")
                .log("Received ORAGLI batch: ${body}")

                .process(exchange -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> records = exchange.getIn().getBody(List.class);

                    if (records == null || records.isEmpty()) {
//                        throw new IllegalArgumentException("No ORAGLI records to process");
                        log.warn("No ORAGLI records found to process");
                        exchange.getIn().setBody(Collections.emptyList());
                        return;
                    }

                    String sql = "INSERT INTO ORAGLI (ITFFIL, ITFBCH, ITFDSC, ITFJRN, ITFJRD, ITFACD, ITFCTN, ITFGLC, ITFGLM, ITFDEP, ITFSTR, ITFAMT, ITFJEN, ITFJLD, ITFREF) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                    List<Object[]> batchParams = new ArrayList<>();

                    for (Map<String, Object> rec : records) {
                        Object[] params = new Object[]{
                                rec.get("ITFFIL"),                     // CHAR(45)
                                rec.get("ITFBCH"),                     // CHAR(24)
                                rec.get("ITFDSC"),                     // CHAR(90)
                                rec.get("ITFJRN"),                     // CHAR(24)
                                rec.get("ITFJRD"),                     // CHAR(90)
                                convertToNumber(rec.get("ITFACD")),   // NUMBER(6)
                                rec.get("ITFCTN"),                     // CHAR(12)
                                convertToNumber(rec.get("ITFGLC")),   // NUMBER(3)
                                convertToNumber(rec.get("ITFGLM")),   // NUMBER(3)
                                convertToNumber(rec.get("ITFDEP")),   // NUMBER(3)
                                convertToNumber(rec.get("ITFSTR")),   // NUMBER(5)
                                rec.get("ITFAMT"),                     // CHAR(51)
                                convertToNumber(rec.get("ITFJEN")),   // NUMBER(5)
                                rec.get("ITFJLD"),                     // CHAR(90)
                                rec.get("ITFREF")                      // CHAR(36)
                        };
                        batchParams.add(params);
                    }

                    int[] results = jdbcTemplate.batchUpdate(sql, batchParams);
                    log.info("Inserted {} ORAGLI records successfully", results.length);

                    exchange.getIn().setHeader("TotalInserted", results.length);
                })

                .process(exchange -> {
                    Integer total = exchange.getIn().getHeader("TotalInserted", Integer.class);
                    String response = String.format("{\"status\":\"success\",\"message\":\"Inserted %d ORAGLI records\"}", total);
                    exchange.getIn().setBody(response);
                })

                .log("Finished ORAGLI batch insert");
    }
}
