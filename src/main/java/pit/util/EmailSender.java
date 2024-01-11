package pit.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pit.etl.config.Config;

public class EmailSender {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private String smtpHost;
    private String from;
    @SuppressWarnings("FieldCanBeLocal")
    private String successTo;
    private String failureTo;
    private String bounceAddress;
    private String smtpUsername;
    private String smtpPassword;

    private boolean isEmailSendingEnabled = true;

    private Config config;

    public EmailSender(Config config) {
        this.config = config;
        smtpHost = config.get("smtp.host");
        from = config.get("email.from");
        successTo = config.get("email.success.to");
        failureTo = config.get("email.failure.to");
        bounceAddress = config.get("email.bounce.address");
        smtpUsername = config.get("smptp.username");
        smtpPassword = config.get("smptp.password");

        isEmailSendingEnabled = config.isTrue("email.enabled");
    }

    private void send(String to, String subject, String text) {
        try {
            // Create the email message
            Email email = new SimpleEmail();
            email.setHostName(smtpHost);
            String[] toAddresses = StringUtils.splitByWholeSeparator(to, ",");
            for (String toAddress : toAddresses) {
                email.addTo(toAddress.trim());
            }
            email.setFrom(from);
            email.setBounceAddress(bounceAddress);
            email.setSubject(subject);
            if (smtpUsername != null && smtpUsername.trim().length() > 0)
                email.setAuthentication(smtpUsername, smtpPassword);

            // set the html message
            email.setMsg(text);
            // send the email
            if (isEmailSendingEnabled) {
                email.send();
            } else {
                logger.info("Email notification is not enabled in the env {}, the email won't be sent", config.env());
            }
            logger.info("Sent email '{}' to '{}'", subject, to);
        } catch (EmailException e) {
            throw new RuntimeException(e);
        }
    }

    public void sendErrorNotification(String subject, Exception ex) {
        subject += " " + ex.getMessage() + " Env: " + config.env() + " Host: " + Utils.getHostname();
        String body = ExceptionUtils.getStackTrace(ex);
        if (StringUtils.isNotBlank(failureTo))
            send(failureTo, subject, body);
        else {
            logger.warn("Email distro for error notification is not defined");
        }

    }


}
