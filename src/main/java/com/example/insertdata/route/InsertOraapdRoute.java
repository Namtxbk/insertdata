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
public class InsertOraapdRoute extends RouteBuilder {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;
    private final LogUtility logUtility;

    private final String mmsUser = "SYSTERM";
    private final String mmsJob = "ITF001JQ";
    private final String mmsProgram = "";
    private final String mmsMember = "";
    private final String mmsTableCode = "ORAAPD";

    public InsertOraapdRoute(DataSource dataSource, LogUtility logUtility) {
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
                .log("Error in ORAAPD batch insert: ${exception.message}")
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
                .process(exchange -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> records = exchange.getIn().getBody(List.class);
                    logUtility.logStart(exchange, records, mmsUser, mmsJob, mmsProgram, mmsMember, mmsTableCode);
                })
                .choice()
                .when(simple("${body} contains 'skipped'"))
                .stop()
                .end()
                .log("Received ORAAPD batch: ${body}")
                .process(exchange -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> records = exchange.getIn().getBody(List.class);

                    String sql = "INSERT INTO ORAAPD (ITFFIL, ITFPFN, ITFMBR, ITFINV, ITFVNO, ITFSEQ, ITFDMT, ITFADT, ITFDST, ITFRCR, ITFPOB, ITFBCH, ITFSTR, ITFCCD, ITFCSQ) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    List<Object[]> batchParams = new ArrayList<>();

                    for (Map<String, Object> rec : records) {
                        Object[] params = new Object[]{
                                rec.get("ITFFIL"),
                                rec.get("ITFPFN"),
                                rec.get("ITFMBR"),
                                rec.get("ITFINV"),
                                convertToNumber(rec.get("ITFVNO")),
                                convertToNumber(rec.get("ITFSEQ")),
                                rec.get("ITFDMT"),
                                convertToNumber(rec.get("ITFADT")),
                                rec.get("ITFDST"),
                                convertToNumber(rec.get("ITFRCR")),
                                convertToNumber(rec.get("ITFPOB")),
                                rec.get("ITFBCH"),
                                convertToNumber(rec.get("ITFSTR")),
                                rec.get("ITFCCD"),
                                convertToNumber(rec.get("ITFCSQ"))
                        };
                        batchParams.add(params);
                    }

                    int[] results = jdbcTemplate.batchUpdate(sql, batchParams);
                    log.info("Inserted {} ORAAPD records successfully", results.length);

                    //                    logUtility.logComplete(exchange);


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
