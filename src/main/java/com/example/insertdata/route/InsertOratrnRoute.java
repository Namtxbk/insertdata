package com.example.insertdata.route;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class InsertOratrnRoute extends RouteBuilder {

    private final JdbcTemplate jdbcTemplate;

    public InsertOratrnRoute(DataSource dataSource) {
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
                .log("Error in ORATRN batch insert: ${exception.stacktrace}")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(500))
                .setBody(simple("{\"error\":\"${exception.message}\"}"))
                .rollback();

        from("direct:processOratrn")
                .routeId("oratrn-insert-route")
                .transacted("PROPAGATION_REQUIRED")
                .log("Received ORATRN batch: ${body}")

                .process(exchange -> {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> records = exchange.getIn().getBody(List.class);

                    if (records == null || records.isEmpty()) {
                        throw new IllegalArgumentException("No ORATRN records to process");
                    }

                    String sql = "INSERT INTO ORATRN (ITFFIL, ITFLAG, ITHCOD, ITRLTP, ITRLOC, ITRCEN, ITRDAT, ITRTYP, INUMBR, ITRQTY, ITRRET, ITRCST, IDEPT, ITRREF, LGUSER, ITCOMP, ITRSDT, ITRSTM) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                    List<Object[]> batchParams = new ArrayList<>();

                    for (Map<String, Object> rec : records) {
                        Object[] params = new Object[] {
                                rec.get("ITFFIL"),                        // CHAR(45)
                                rec.get("ITFLAG"),                        // CHAR(3)
                                rec.get("ITHCOD"),                        // CHAR(9)
                                rec.get("ITRLTP"),                        // CHAR(3)
                                convertToNumber(rec.get("ITRLOC")),      // NUMBER(5)
                                convertToNumber(rec.get("ITRCEN")),      // NUMBER(1)
                                convertToNumber(rec.get("ITRDAT")),      // NUMBER(6)
                                convertToNumber(rec.get("ITRTYP")),      // NUMBER(3)
                                convertToNumber(rec.get("INUMBR")),      // NUMBER(9)
                                convertToNumber(rec.get("ITRQTY")),      // NUMBER(10)
                                convertToNumber(rec.get("ITRRET")),      // NUMBER(15)
                                convertToNumber(rec.get("ITRCST")),      // NUMBER(15)
                                convertToNumber(rec.get("IDEPT")),       // NUMBER(3)
                                rec.get("ITRREF"),                        // CHAR(45)
                                rec.get("LGUSER"),                        // CHAR(30)
                                convertToNumber(rec.get("ITCOMP")),      // NUMBER(3)
                                convertToNumber(rec.get("ITRSDT")),      // NUMBER(6)
                                convertToNumber(rec.get("ITRSTM"))       // NUMBER(6)
                        };
                        batchParams.add(params);
                    }

                    int[] results = jdbcTemplate.batchUpdate(sql, batchParams);
                    exchange.getIn().setHeader("TotalInserted", results.length);
                })

                .process(exchange -> {
                    Integer total = exchange.getIn().getHeader("TotalInserted", Integer.class);
                    String response = String.format("{\"status\":\"success\",\"message\":\"Inserted %d ORATRN records\"}", total);
                    exchange.getIn().setBody(response);
                })

                .log("Finished ORATRN batch insert");
    }
}
