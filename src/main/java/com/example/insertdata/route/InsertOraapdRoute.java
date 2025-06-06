package com.example.insertdata.route;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class InsertOraapdRoute extends RouteBuilder {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

    public InsertOraapdRoute(DataSource dataSource) {
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
                .log("Error in ORAAPD batch insert: ${exception.stacktrace}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setBody(simple("{\"error\":\"${exception.message}\"}"))
                .rollback();

        restConfiguration()
                .component("servlet")
                .bindingMode(RestBindingMode.json)
                .dataFormatProperty("prettyPrint", "true")
                .contextPath("/api")
                .port(8080);

        rest("/oraapd")
                .post("/batch")
                .consumes("application/json")
                .produces("application/json")
                .to("direct:processOraapd");

        from("direct:processOraapd")
                .routeId("oraapd-insert-route")
                .transacted("PROPAGATION_REQUIRED")
                .log("Received ORAAPD batch: ${body}")

                .process(exchange -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> records = exchange.getIn().getBody(List.class);

                    if (records == null || records.isEmpty()) {
                        throw new IllegalArgumentException("No ORAAPD records to process");
                    }

                    String sql = "INSERT INTO ORAAPD (ITFFIL, ITFPFN, ITFMBR, ITFINV, ITFVNO, ITFSEQ, ITFDMT, ITFADT, ITFDST, ITFRCR, ITFPOB, ITFBCH, ITFSTR, ITFCCD, ITFCSQ) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                    List<Object[]> batchParams = new ArrayList<>();

                    for (Map<String, Object> rec : records) {
                        Object[] params = new Object[]{
                                rec.get("ITFFIL"),                     // CHAR(45)
                                rec.get("ITFPFN"),                     // CHAR(30)
                                rec.get("ITFMBR"),                     // CHAR(30)
                                rec.get("ITFINV"),                     // CHAR(60)
                                convertToNumber(rec.get("ITFVNO")),   // NUMBER(6)
                                convertToNumber(rec.get("ITFSEQ")),   // NUMBER(5)
                                rec.get("ITFDMT"),                     // CHAR(51)
                                convertToNumber(rec.get("ITFADT")),   // NUMBER(6)
                                rec.get("ITFDST"),                     // CHAR(42)
                                convertToNumber(rec.get("ITFRCR")),   // NUMBER(10)
                                convertToNumber(rec.get("ITFPOB")),   // NUMBER(12)
                                rec.get("ITFBCH"),                     // CHAR(24)
                                convertToNumber(rec.get("ITFSTR")),   // NUMBER(5)
                                rec.get("ITFCCD"),                     // CHAR(6)
                                convertToNumber(rec.get("ITFCSQ"))    // NUMBER(3)
                        };
                        batchParams.add(params);
                    }

                    int[] results = jdbcTemplate.batchUpdate(sql, batchParams);
                    log.info("Inserted {} ORAAPD records successfully", results.length);

                    exchange.getIn().setHeader("TotalInserted", results.length);
                })

                .process(exchange -> {
                    Integer total = exchange.getIn().getHeader("TotalInserted", Integer.class);
                    String response = String.format("{\"status\":\"success\",\"message\":\"Inserted %d ORAAPD records\"}", total);
                    exchange.getIn().setBody(response);
                })

                .log("Finished ORAAPD batch insert");
    }
}
