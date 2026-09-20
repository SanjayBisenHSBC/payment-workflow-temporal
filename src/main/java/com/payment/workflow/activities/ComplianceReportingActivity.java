package com.payment.workflow.activities;

import com.payment.workflow.model.PaymentContext;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface ComplianceReportingActivity {
    @ActivityMethod
    PaymentContext fileCrossBorderReport(PaymentContext context);
}
