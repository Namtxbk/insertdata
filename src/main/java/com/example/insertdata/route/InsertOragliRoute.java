package com.example.insertdata.route;

import com.example.insertdata.ultis.LogUtility;
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
public class InsertOragliRoute extends RouteBuilder {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final LogUtility logUtility;

    private final String mmsUser = "SYSTERM";
    private final String mmsJob = "ITF001JQ";
    private final String mmsProgram = "ITF99Z";
    private final String mmsMember = "R0056882";
    private final String mmsTableCode = "ORAGLI";

    public InsertOragliRoute(DataSource dataSource, LogUtility logUtility) {
        this.dataSource = dataSource;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.logUtility = logUtility;
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
                log.warn("Invalid number format for value: {}", str);
                return null;
            }
        }
        return value;
    }

    @Override
    public void configure() throws Exception {
        onException(Exception.class)
                .handled(true)
                .log("Error occurred: ${exception.message}")
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
                .transacted("PROPAGATION_REQUIRED")   .process(exchange -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> records = exchange.getIn().getBody(List.class);
                    logUtility.logStart(exchange, records, mmsUser, mmsJob, mmsProgram, mmsMember, mmsTableCode);
                })
                .choice()
                .when(simple("${body} contains 'skipped'"))
                .stop()
                .end()

                .log("Received ORAGLI batch: ${body}")
                .process(exchange -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> records = exchange.getIn().getBody(List.class);

                    String sql = "INSERT INTO ORAGLI (ITFFIL, ITFBCH, ITFDSC, ITFJRN, ITFJRD, ITFACD, ITFCTN, ITFGLC, ITFGLM, ITFDEP, ITFSTR, ITFAMT, ITFJEN, ITFJLD, ITFREF) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    List<Object[]> batchParams = new ArrayList<>();

                    for (Map<String, Object> rec : records) {
                        Object[] params = new Object[]{
                                rec.get("ITFFIL"),
                                rec.get("ITFBCH"),
                                rec.get("ITFDSC"),
                                rec.get("ITFJRN"),
                                rec.get("ITFJRD"),
                                convertToNumber(rec.get("ITFACD")),
                                rec.get("ITFCTN"),
                                convertToNumber(rec.get("ITFGLC")),
                                convertToNumber(rec.get("ITFGLM")),
                                convertToNumber(rec.get("ITFDEP")),
                                convertToNumber(rec.get("ITFSTR")),
                                rec.get("ITFAMT"),
                                convertToNumber(rec.get("ITFJEN")),
                                rec.get("ITFJLD"),
                                rec.get("ITFREF")
                        };
                        batchParams.add(params);
                    }

                    int[] results = jdbcTemplate.batchUpdate(sql, batchParams);
                    log.info("Inserted {} ORAGLI records successfully", results.length);
                    //                    logUtility.logComplete(exchange);


                    
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