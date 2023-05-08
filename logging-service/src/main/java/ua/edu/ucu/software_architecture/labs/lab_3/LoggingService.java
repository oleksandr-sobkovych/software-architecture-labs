package ua.edu.ucu.software_architecture.labs.lab_3;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;

public class LoggingService {
    private static final Logger logger = Logger.getLogger(LoggingService.class.getName());

    public static void main(String[] args) throws InterruptedException, IOException {
        int port;
        try {
            port = Integer.parseInt(System.getenv("LOGGING_SERVICE_PORT"));
        } catch (Exception e) {
            throw new IOException("Invalid port supplied to service");
        }

        logger.setLevel(Level.INFO);
        Server server;
        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(new HazelcastMessageLogger())
                .build()
                .start();
        logger.info("Starting the service at " + InetAddress.getLocalHost() + ":" + port);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.shutdown();
                try {
                    if (!server.awaitTermination(30, TimeUnit.SECONDS)) {
                        server.shutdownNow();
                        server.awaitTermination(5, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException ex) {
                    server.shutdownNow();
                }
            }
        });

        server.awaitTermination();
    }
}
