package com.payment.workflow.activities;

import com.payment.workflow.model.PaymentContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component("complianceReportingActivity")
@RequiredArgsConstructor
public class ComplianceReportingActivityImpl implements ComplianceReportingActivity{
    @Override
    public PaymentContext fileCrossBorderReport(PaymentContext context) {
        return null;
    }
}
