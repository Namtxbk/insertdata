package com.example.insertdata.route;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

@Component
public class InsertOraaphRoute extends RouteBuilder {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

    public InsertOraaphRoute(DataSource dataSource) {
        this.dataSource = dataSource;
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    private Object convertToNumber(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            String str = (String) value;
            if (str.isEmpty()) {
                return null;
            }
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

        // REST Configuration
        restConfiguration()
                .component("servlet")
                .bindingMode(RestBindingMode.json)
                .dataFormatProperty("prettyPrint", "true")
                .contextPath("/api")
                .port(8080);

        // REST Endpoint
        rest("/invoices")
                .post("/batch")
                .consumes("application/json")
                .produces("application/json")
                .to("direct:processInvoices");

        from("direct:processInvoices")
                .log("Received request: ${body}")
                .routeId("invoice-processing-route")
                .transacted("PROPAGATION_REQUIRED")

                .process(exchange -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> invoices = exchange.getIn().getBody(List.class);

                    if (invoices == null || invoices.isEmpty()) {
//                        throw new IllegalArgumentException("No invoices to process");

                        log.warn("No Oraaph records found to process");
                        exchange.getIn().setBody(Collections.emptyList());
                        return;
                    }

                    String sql = "INSERT INTO ORAAPH (ITFFIL, ITFPFN, ITFMBR, ITFINV, ITFCRD, ITFDTE, ITFVNO, ITFCMP, ITFAMT, ITFTRM, ITFDSC, ITFRDT, ITFIDT, ITFGDT, ITFREB, ITFBCH, ITFSTR, ITFCUR, ITFXRT, ITFMDV) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                    // Batch insert
                    List<Object[]> batchParams = new ArrayList<>();

                    for (Map<String, Object> invoice : invoices) {
                        Object[] params = new Object[]{
                                invoice.get("ITFFIL"),
                                invoice.get("ITFPFN"),
                                invoice.get("ITFMBR"),
                                invoice.get("ITFINV"),
                                invoice.get("ITFCRD"),
                                convertToNumber(invoice.get("ITFDTE")),
                                convertToNumber(invoice.get("ITFVNO")),
                                convertToNumber(invoice.get("ITFCMP")),
                                convertToNumber(invoice.get("ITFAMT")),
                                convertToNumber(invoice.get("ITFTRM")),
                                invoice.get("ITFDSC"),
                                convertToNumber(invoice.get("ITFRDT")),
                                convertToNumber(invoice.get("ITFIDT")),
                                convertToNumber(invoice.get("ITFGDT")),
                                invoice.get("ITFREB"),
                                invoice.get("ITFBCH"),
                                convertToNumber(invoice.get("ITFSTR")),
                                invoice.get("ITFCUR"),
                                convertToNumber(invoice.get("ITFXRT")),
                                convertToNumber(invoice.get("ITFMDV"))
                        };
                        batchParams.add(params);
                    }

                    int[] results = jdbcTemplate.batchUpdate(sql, batchParams);

                    log.info("Batch insert completed. Inserted {} records", results.length);
                    exchange.getIn().setHeader("TotalInvoices", results.length);
                })

                .process(exchange -> {
                    Integer totalInvoices = exchange.getIn().getHeader("TotalInvoices", Integer.class);
                    String response = String.format(
                            "{\"status\":\"success\", \"message\":\"Successfully processed %d invoices\"}",
                            totalInvoices
                    );
                    exchange.getIn().setBody(response);
                })
                .log("Batch processing completed successfully");
    }
}