package ua.edu.ucu.software_architecture.labs.lab_3;

import java.util.UUID;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.logging.Level;
import java.util.logging.Logger;

import ua.edu.ucu.software_architecture.labs.log_message.GetAllReply;
import ua.edu.ucu.software_architecture.labs.log_message.LogReply;
import ua.edu.ucu.software_architecture.labs.log_message.MsgAllGetRequest;
import ua.edu.ucu.software_architecture.labs.log_message.MsgPostRequest;
import ua.edu.ucu.software_architecture.labs.log_message.MessageLoggerGrpc;

public final class HazelcastMessageLogger extends MessageLoggerGrpc.MessageLoggerImplBase {
  private static final Logger logger = Logger.getLogger(HazelcastMessageLogger.class.getName());
  private final IMap<UUID, String> msgLoggerMap;

  public HazelcastMessageLogger() {
    logger.setLevel(Level.INFO);
    Config mapStoreConfig = new Config();
    mapStoreConfig.setClusterName("dev");

    logger.info("Starting embedded hazelcast instance...");
    HazelcastInstance hzMapClient = Hazelcast.newHazelcastInstance(mapStoreConfig);
    hzMapClient.getMap("message-logger").destroy();
    this.msgLoggerMap = hzMapClient.getMap("message-logger");
  }

  @Override
  public void logMessage(MsgPostRequest request, StreamObserver<LogReply> responseObserver) {
    logger.info("Got a request: " + request.toString());

    try {
      UUID msgUuid = UUID.fromString(request.getUuid());
      String msgContent = request.getMessage();
      if (msgUuid == null || msgContent == null) {
        logger.severe("Got a NULL request");
        responseObserver
            .onError(Status.INVALID_ARGUMENT.withDescription("Bad UUID or message in request").asRuntimeException());
      } else {
        msgLoggerMap.set(msgUuid, msgContent);
        responseObserver.onNext(LogReply.newBuilder().build());
        responseObserver.onCompleted();
      }
    } catch (Exception e) {
      logger.severe(e.getClass() + ": " + e.getMessage());
      responseObserver
          .onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }
  }

  @Override
  public void getAllMessages(MsgAllGetRequest request, StreamObserver<GetAllReply> responseObserver) {
    logger.info("Got a request: " + request.toString());

    try {
      responseObserver.onNext(GetAllReply
          .newBuilder()
          .setMessages(String.join(":", msgLoggerMap.values()))
          .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.severe(e.getClass() + ": " + e.getMessage());
      responseObserver
          .onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }
  }
}
