package pit.util;

import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.regex.Pattern;

public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static String getHostname() {
        String hostName = null;
        try {
            final String HOSTNAME = "hostname";
            @SuppressWarnings("resource")
            java.util.Scanner s = new java.util.Scanner(Runtime.getRuntime().exec(HOSTNAME).getInputStream()).useDelimiter("\\A");
            if (s.hasNext() ) {
                hostName = s.next();
            }
        } catch (IOException e) {
            logger.warn("Unable to obtain a hostname: " + e.getMessage());
        }

        return hostName;
    }

    @SuppressWarnings("unused") public static String match(String pattern, String target) {
        return target == null ? null : Pattern.compile(pattern).matcher(target).find() ? target : null;
    }


    @SneakyThrows
    public static String createHash(String stringToHash) {
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        // 0x9F86D081884C7D659A2FEAA0C55AD015A3BF4F1B2B0B822CD15D6C15B0F00A08
        final byte[] hashbytes = digest.digest(
                stringToHash.getBytes(StandardCharsets.UTF_8));
        return bytesToHex(hashbytes);
    }


    public static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte h : hash) {
            String hex = Integer.toHexString(0xff & h);
            if (hex.length() == 1)
                hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString().toUpperCase();
    }
}