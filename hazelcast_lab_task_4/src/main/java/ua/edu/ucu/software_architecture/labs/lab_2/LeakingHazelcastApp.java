package ua.edu.ucu.software_architecture.labs.lab_2;

import java.io.Serializable;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class LeakingHazelcastApp implements Runnable {
    static final int iterationsNum = 1000;
    static final int threadsNum = 3;

    static class Value implements Serializable {
        public int amount;
    }

    @Override
    public void run() {
        ClientConfig mapTestConfig = new ClientConfig();
        mapTestConfig.setClusterName("dev");
        mapTestConfig.getNetworkConfig().addAddress("localhost");

        HazelcastInstance hzMapClient = HazelcastClient.newHazelcastClient(mapTestConfig);

        IMap<String, Value> mapValues = hzMapClient.getMap("huge-map");
        String key = "1";
        mapValues.put(key, new Value());
        System.out.println("Starting");
        for (int k = 0; k < iterationsNum; k++) {
            if (k % 100 == 0)
                System.out.println("At: " + k);
            Value value = mapValues.get(key);
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                System.err.println("Thread interrupted");
            }
            value.amount++;
            mapValues.put(key, value);
        }
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            System.err.println("Thread interrupted");
        }
        System.out.println("Finished! Result = " + mapValues.get(key).amount);

        hzMapClient.shutdown();
    }

    public static void main(String[] args) {
        ClientConfig mapTestConfig = new ClientConfig();
        mapTestConfig.setClusterName("dev");
        mapTestConfig.getNetworkConfig().addAddress("localhost");

        HazelcastInstance hzMapClient = HazelcastClient.newHazelcastClient(mapTestConfig);

        // destroy the map if exists
        hzMapClient.getMap("huge-map").destroy();

        for (int i = 0; i < threadsNum; i++) {
            Thread threadHandle = new Thread(new LeakingHazelcastApp());
            threadHandle.start();
        }

        hzMapClient.shutdown();
    }
}
