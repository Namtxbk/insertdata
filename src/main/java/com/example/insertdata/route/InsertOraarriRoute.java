package com.example.insertdata.route;


import com.example.insertdata.ultis.LogUtility;
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
public class InsertOraarriRoute extends RouteBuilder {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final LogUtility logUtility ;

    private final String mmsUser = "SYSTERM";
    private final String mmsJob = "ITF030JQ";
    private final String mmsProgram = "";
    private final String mmsMember = "";
    private final String mmsTableCode = "ORAARRI";

    public InsertOraarriRoute(DataSource dataSource,  LogUtility logUtility) {
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
                return null;
            }
        }
        return value;
    }

    @Override
    public void configure() throws Exception {
        onException(Exception.class)
                .handled(true)
                .log("Error in ORAARRI batch insert: ${exception.stacktrace}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setBody(simple("{\"error\":\"${exception.message}\"}"))
                .rollback();

        restConfiguration()
                .component("servlet")
                .bindingMode(RestBindingMode.json)
                .dataFormatProperty("prettyPrint", "true")
                .contextPath("/api")
                .port(8080);

        rest("/oraarri")
                .post("/batch")
                .consumes("application/json")
                .produces("application/json")
                .to("direct:processOraarri");

        from("direct:processOraarri")
                .routeId("oraarri-insert-route")
                .transacted("PROPAGATION_REQUIRED")
                .process(exchange -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> records = exchange.getIn().getBody(List.class);
                    logUtility.logStart(exchange, records, mmsUser, mmsJob, mmsProgram, mmsMember, mmsTableCode);
                })
                .choice()
                .when(simple("${body} contains 'skipped'"))
                .stop()
                .end()
                .log("Received ORAARRI batch: ${body}")

                .process(exchange -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> records = exchange.getIn().getBody(List.class);

                    if (records == null || records.isEmpty()) {
//                        throw new IllegalArgumentException("No ORAARRI records to process");
                        log.warn("No ORAARRI records found to process");
                        exchange.getIn().setBody(Collections.emptyList());
                        return;
                    }

                    String sql = "INSERT INTO ORAARRI (ITFFIL, ITFTRN, ITFAMT, ITFDTE, ITFTYP, ITFCUS, ITFMDT, ITFSEQ, ITFGCO, ITFDOC, ITFSTR, ITFTIL) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                    List<Object[]> batchParams = new ArrayList<>();

                    for (Map<String, Object> rec : records) {
                        Object[] params = new Object[]{
                                rec.get("ITFFIL"),                     // CHAR(66)
                                rec.get("ITFTRN"),                     // CHAR(66)
                                rec.get("ITFAMT"),                     // CHAR(51)
                                convertToNumber(rec.get("ITFDTE")),   // NUMBER(6)
                                rec.get("ITFTYP"),                     // CHAR(6)
                                convertToNumber(rec.get("ITFCUS")),   // NUMBER(10)
                                convertToNumber(rec.get("ITFMDT")),   // NUMBER(6)
                                convertToNumber(rec.get("ITFSEQ")),   // NUMBER(3)
                                convertToNumber(rec.get("ITFGCO")),   // NUMBER(3)
                                rec.get("ITFDOC"),                     // CHAR(60)
                                convertToNumber(rec.get("ITFSTR")),   // NUMBER(5)
                                convertToNumber(rec.get("ITFTIL"))    // NUMBER(5)
                        };
                        batchParams.add(params);
                    }

                    int[] results = jdbcTemplate.batchUpdate(sql, batchParams);
                    log.info("Inserted {} ORAARRI records successfully", results.length);
                    //                    logUtility.logComplete(exchange);

                    exchange.getIn().setHeader("TotalInserted", results.length);
                })

                .process(exchange -> {
                    Integer total = exchange.getIn().getHeader("TotalInserted", Integer.class);
                    String response = String.format("{\"status\":\"success\",\"message\":\"Inserted %d ORAARRI records\"}", total);
                    exchange.getIn().setBody(response);
                })

                .log("Finished ORAARRI batch insert");
    }
}
