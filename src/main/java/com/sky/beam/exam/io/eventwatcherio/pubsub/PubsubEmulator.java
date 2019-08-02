package com.sky.beam.exam.io.eventwatcherio.pubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.Topic;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;

public class PubsubEmulator {

    public static void main(String[] args) throws IOException {
//        createTopic("video-events");
        createTopic("content-events");
    }

    public static void createTopic(String topicNameStr) throws IOException {
        String hostport = "127.0.0.1:8085";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
        try {
            TransportChannelProvider channelProvider =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            CredentialsProvider credentialsProvider = NoCredentialsProvider.create();

            TopicAdminClient topicClient =
                    TopicAdminClient.create(
                            TopicAdminSettings.newBuilder()
                                    .setTransportChannelProvider(channelProvider)
                                    .setCredentialsProvider(credentialsProvider)
                                    .build());

            ProjectTopicName topicName = ProjectTopicName.of("crucial-module-223618", topicNameStr);     // "sky-project"

            try {
                Topic response = topicClient.createTopic(topicName);  //"video-events");
                System.out.printf("Topic %s created.\n", response);
            } catch (ApiException e) {
                System.out.println(e.getStatusCode().getCode());
                System.out.println(e.isRetryable());
                System.out.println("No topic was created.");
            }

        } finally {
            channel.shutdown();
        }


    }


}
