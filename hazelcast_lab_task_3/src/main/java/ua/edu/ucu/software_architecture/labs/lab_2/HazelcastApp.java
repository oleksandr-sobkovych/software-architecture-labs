package ua.edu.ucu.software_architecture.labs.lab_2;

import java.util.Scanner;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class HazelcastApp {
    static final int maxMapItems = 1000;
    static final int stringRepetitions = 10;

    private static void assertMap(IMap<Integer, String> mapNumbers) throws AssertionError {
        System.out.println("Asserting " + maxMapItems + " items...");
        for (int i = 0; i < maxMapItems; i++) {
            assert mapNumbers.get(i).equals(Integer.toString(i).repeat(stringRepetitions));
        }
        System.out.println("No data loss occured");
    }

    public static void main(String[] args) {
        ClientConfig mapTestConfig = new ClientConfig();
        mapTestConfig.setClusterName("dev");
        mapTestConfig.getNetworkConfig().addAddress("localhost");

        HazelcastInstance hzMapClient = HazelcastClient.newHazelcastClient(mapTestConfig);

        Scanner consoleWaiter = new Scanner(System.in);

        // destroy the map if exists
        hzMapClient.getMap("huge-map").destroy();
        IMap<Integer, String> mapNumbers = hzMapClient.getMap("huge-map");

        System.out.println("Adding " + maxMapItems + " items...");
        for (int i = 0; i < maxMapItems; i++) {
            mapNumbers.put(i, Integer.toString(i).repeat(stringRepetitions));
        }

        try {
            assertMap(mapNumbers);

            System.out.println("Press 'Enter' to continue...");
            consoleWaiter.nextLine();

            assertMap(mapNumbers);
        } catch (AssertionError e) {
            // print to stdout for simple management (no need to search in logs)
            System.out.println("Data loss occured");
        } finally {
            consoleWaiter.close();
            hzMapClient.shutdown();
        }
    }
}
