package ua.edu.ucu.software_architecture.labs.lab_2;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.HazelcastInstance;

public class ConsumerApp {

    public static void main( String[] args ) throws Exception {
        ClientConfig queueInstanceConfig = new ClientConfig();
        queueInstanceConfig.setClusterName("dev");
        queueInstanceConfig.getNetworkConfig().addAddress("localhost");

        HazelcastInstance hzQueueClient = HazelcastClient.newHazelcastClient(queueInstanceConfig);
        QueueConfig queueConfig = new QueueConfig("testQueue");
        queueConfig.setMaxSize(10);
        hzQueueClient.getConfig().addQueueConfig(queueConfig);

        IQueue<Integer> queue = hzQueueClient.getQueue( "testQueue" );
        while ( true ) {
            int item = queue.take();
            System.out.println( "Consumed: " + item );
            if ( item == -1 ) {
                queue.put( -1 );
                break;
            }
            Thread.sleep( 2500 );
        }
        System.out.println( "Consumer Finished!" );
    }
}
