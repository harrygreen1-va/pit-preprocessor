package pit.ssltest;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import javax.net.ssl.*;
import java.io.*;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class SslConnectionTester {


//    final static String TRUST_STORE_PROP = "javax.net.ssl.trustStore";
//    final static String TRUST_STORE_PASSWORD_PROP = "javax.net.ssl.trustStorePassword";
//
//    final static String TRUST_STORE_PASSWORD = "app-trust";

    @BeforeClass
    public static void setup() {
//        System.setProperty(TRUST_STORE_PROP, "./src/test/resources/com/myarch/sec/cryptofiles/app_truststore.pkcs12");
//        System.setProperty(TRUST_STORE_PASSWORD_PROP, TRUST_STORE_PASSWORD);
    }

    @Ignore
    @Test
    public void testConnection() throws IOException, NoSuchAlgorithmException, KeyManagementException {
//        System.setProperty("javax.net.debug", "ssl,handshake,trustmanager");
//        System.setProperty("javax.net.debug", "all");
        SSLContext context = SSLContext.getInstance("TLS");
        X509TrustingManager tm = new X509TrustingManager();
//        context.init(new KeyManager[]{null}, new TrustManager[]{tm}, null);
        connect("vaausnodpci200b.aac.dva.va.gov", 1433);
    }
/*
			SSLContext context = SSLContext.getInstance("TLS");
			X509TrustingManager tm = new X509TrustingManager();
			context.init(new KeyManager[] { km }, new TrustManager[] { tm }, null);

 */

    public void connect(String host, int port) throws IOException {

        SSLSocketFactory factory =
                (SSLSocketFactory) SSLSocketFactory.getDefault();

        SSLSocket socket =
                (SSLSocket) factory.createSocket(host, port);

        /*
         * register a callback for handshaking completion event
         */
        socket.addHandshakeCompletedListener(
                new HandshakeCompletedListener() {
                    public void handshakeCompleted(
                            HandshakeCompletedEvent event) {
                        System.out.println("Handshake finished!");
                        System.out.println(
                                "\t CipherSuite:" + event.getCipherSuite());
                        System.out.println(
                                "\t SessionId " + event.getSession());
                        System.out.println(
                                "\t PeerHost " + event.getSession().getPeerHost());
                    }
                }
        );

        /*
         * send http request
         *
         * See SSLSocketClient.java for more information about why
         * there is a forced handshake here when using PrintWriters.
         */
        socket.startHandshake();

        PrintWriter out = new PrintWriter(
                new BufferedWriter(
                        new OutputStreamWriter(
                                socket.getOutputStream())));

        out.println("GET / HTTP/1.0");
        out.println();
        out.flush();

        /*
         * Make sure there were no surprises
         */
        if (out.checkError())
            System.out.println(
                    "SSLSocketClient:  java.io.PrintWriter error");

        /* read response */
        BufferedReader in = new BufferedReader(
                new InputStreamReader(
                        socket.getInputStream()));

        String inputLine;
        StringBuilder responseContent = new StringBuilder();
        while ((inputLine = in.readLine()) != null)
            responseContent.append(inputLine).append('\n');

        in.close();
        out.close();
        socket.close();
    }

    private static class X509TrustingManager implements X509TrustManager {
        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            throw new UnsupportedOperationException();
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            System.err.println("All are trusted");
            return new X509Certificate[0];
        }
    }

}
