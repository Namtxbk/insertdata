package com.example.insertdata.controller;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class InvoiceController {

    private final CamelContext camelContext;

    public InvoiceController(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    @PostMapping("/oraaph")
    public ResponseEntity<?> processInvoices(@RequestBody List<Map<String, Object>> invoices) {
        try {
            ProducerTemplate template = camelContext.createProducerTemplate();
            Object result = template.requestBody("direct:processInvoices", invoices);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    @PostMapping("/oragli")
    public ResponseEntity<?> processOragli(@RequestBody List<Map<String, Object>> records) {
        try {
            ProducerTemplate template = camelContext.createProducerTemplate();
            Object result = template.requestBody("direct:processOragli", records);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    @PostMapping("/oraapd")
    public ResponseEntity<?> processOraapd(@RequestBody List<Map<String, Object>> records) {
        try {
            ProducerTemplate template = camelContext.createProducerTemplate();
            Object result = template.requestBody("direct:processOraapd", records);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }

    @PostMapping("/oraarri")
    public ResponseEntity<?> processOraarri(@RequestBody List<Map<String, Object>> records) {
        try {
            ProducerTemplate template = camelContext.createProducerTemplate();
            Object result = template.requestBody("direct:processOraarri", records);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }


    @PostMapping("/oratrn")
    public ResponseEntity<?> processOratrn(@RequestBody List<Map<String, Object>> records) {
        try {
            ProducerTemplate template = camelContext.createProducerTemplate();
            Object result = template.requestBody("direct:processOratrn", records);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(500).body("{\"error\":\"" + e.getMessage() + "\"}");
        }
    }



    @Autowired
    private JdbcTemplate jdbcTemplate;

    @GetMapping("/test-db")
    public String testDb() {
        try {
            jdbcTemplate.execute("INSERT INTO ORAAPH(ITFFIL, ITFPFN) VALUES ('TEST111', 'TEST111')");
            return "Thành công - kiểm tra database ngay!";
        } catch (Exception e) {
            return "Lỗi: " + e.getMessage();
        }
    }


    @Autowired
    private PlatformTransactionManager transactionManager;

    @GetMapping("/manual-test")
    public String manualTest() {
        TransactionStatus status = transactionManager.getTransaction(new DefaultTransactionDefinition());
        try {
            jdbcTemplate.update("INSERT INTO ORAAPH(ITFFIL) VALUES ('MANUAL_TEST')");
            transactionManager.commit(status);
            return "Commit thành công - kiểm tra DB ngay!";
        } catch (Exception e) {
            transactionManager.rollback(status);
            return "Đã rollback: " + e.getMessage();
        }
    }
}