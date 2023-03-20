package ua.edu.ucu.software_architecture.labs.lab_2;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;

public class ProducerApp {

    public static void main( String[] args ) throws Exception {
        ClientConfig queueInstanceConfig = new ClientConfig();
        queueInstanceConfig.setClusterName("dev");
        queueInstanceConfig.getNetworkConfig().addAddress("localhost");

        HazelcastInstance hzQueueClient = HazelcastClient.newHazelcastClient(queueInstanceConfig);
        // producer will destroy the queue if present (there can only be a single producer)
        hzQueueClient.getQueue( "testQueue" ).destroy();
        QueueConfig queueConfig = new QueueConfig("testQueue");
        queueConfig.setMaxSize(10);
        hzQueueClient.getConfig().addQueueConfig(queueConfig);

        IQueue<Integer> queue = hzQueueClient.getQueue( "testQueue" );
        for ( int k = 1; k < 100; k++ ) {
            queue.put( k );
            System.out.println( "Producing: " + k );
            Thread.sleep(500);
        }
        queue.put( -1 );
        System.out.println( "Producer Finished!" );
    }
}
