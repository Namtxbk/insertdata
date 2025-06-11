package com.example.insertdata.ultis;


import jakarta.transaction.Transactional;
import org.apache.camel.Exchange;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Component
public class LogUtility {

    private final JdbcTemplate jdbcTemplate;

    public LogUtility(DataSource dataSource) {
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
    @Transactional
    public void logStart(Exchange exchange, List<Map<String, Object>> records, String mmsUser, String mmsJob,
                         String mmsProgram, String mmsMember, String mmsTableCode) {
        if (records == null || records.isEmpty()) {
            exchange.getIn().setBody("{\"status\":\"skipped\",\"message\":\"No records to process\"}");
            return;
        }

        Map<String, Object> firstRecord = records.get(0);
        String itffil = (String) firstRecord.get("ITFFIL");
        String itfbch = itffil != null && itffil.contains(".") ? itffil.substring(0, itffil.indexOf(".")) : itffil;
        Object itfglcObj = convertToNumber(firstRecord.get("ITFGLC"));
        String itfglc = itfglcObj != null ? String.valueOf(itfglcObj) : "100";
        int rowCount = records.size();
        String currentDate = new SimpleDateFormat("yyMMdd").format(new Date());
        String currentTime = new SimpleDateFormat("HHmmss").format(new Date());

        if (itffil == null ) {
            throw new IllegalArgumentException("Required fields ITFFIL are missing");
        }
        exchange.setProperty("ITFFIL", itffil);
        exchange.setProperty("ITFBCH", itfbch);
        exchange.setProperty("ITGLCO", itfglc);
        exchange.setProperty("ITFRDN", rowCount);
        exchange.setProperty("ITFDTE", currentDate);
        exchange.setProperty("ITFTIM", currentTime);

        String logSql = "INSERT INTO ITFLOG_UP (ITFCEN, ITFDTE, ITFTIM, ITFUSR, ITFJOB, ITFFIL, ITFPGM, ITFBCH, ITFSEQ, ITGLCO, ITFPFN, ITFMBR, ITFRDN, ITFECN, ITFEDT, ITFETM) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Object[] logParams = new Object[]{
                1, currentDate, currentTime, mmsUser, mmsJob, itffil, mmsProgram, itfbch, 1, itfglc, mmsTableCode, mmsMember, rowCount, 0, null, null
        };
        jdbcTemplate.update(logSql, logParams);
    }
//    @Transactional
//    public void logComplete(Exchange exchange) {
//        String itffil = exchange.getProperty("ITFFIL", String.class);
//        String itfbch = exchange.getProperty("ITFBCH", String.class);
//        if (itffil == null) return;
//
//        String currentDate = new SimpleDateFormat("yyMMdd").format(new Date());
//        String currentTime = new SimpleDateFormat("HHmmss").format(new Date());
//        String updateLogSql = "UPDATE ITFLOG_UP SET ITFECN = ?, ITFEDT = ?, ITFETM = ? WHERE ITFFIL = ? AND ITFBCH = ?";
//        jdbcTemplate.update(updateLogSql, 1, currentDate, currentTime, itffil,itfbch);
//    }
//    @Transactional
//    public void logError(Exchange exchange) {
//        String itffil = exchange.getProperty("ITFFIL", String.class);
//        String itfbch = exchange.getProperty("ITFBCH", String.class);
//        if (itffil == null) return;
//
//        String currentDate = new SimpleDateFormat("yyMMdd").format(new Date());
//        String currentTime = new SimpleDateFormat("HHmmss").format(new Date());
//        String updateLogSql = "UPDATE ITFLOG_UP SET ITFECN = ?, ITFEDT = ?, ITFETM = ? WHERE ITFFIL = ? AND ITFBCH = ?";
//        jdbcTemplate.update(updateLogSql, 0, currentDate, currentTime, itffil,itfbch);
//    }
}
