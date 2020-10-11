package com.redislabs.jedistest;

import org.apache.commons.cli.*;

import java.util.Set;
import java.util.HashSet;
import java.util.Random;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.*;


public class JedisTest 
{
    private static final String HOST = "host";
    private static final String PORT = "port";

    private static void runTest(String host, int port)
    {
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();

        System.out.println("** Starting test run:");
        System.out.println("Host: " + host);
        System.out.println("Port: " + port);

        jedisClusterNodes.add(new HostAndPort(host, port));
        JedisCluster jc = new JedisCluster(jedisClusterNodes);

        long counter = 0;
        long failStart = 0;

        String lastError = "";
        long lastErrorCount = 0;

        Random rand = new Random();

        while (true) {
            try {
                String keyName = "key-" + Integer.toString(rand.nextInt(100000));
                jc.set(keyName, "value-for-" + keyName);
            } catch (JedisException ex) {
                if (failStart == 0) {
                    failStart = java.lang.System.currentTimeMillis();
                    System.out.println("");
                    System.out.println("Cluster is down: ");
                }

                if (!(lastError.equals(ex.getMessage()))) {
                    if (lastError.length() > 0) {
                        System.out.println("");
                    }
                    lastError = ex.getMessage();
                    lastErrorCount = 1;
                } else {
                    lastErrorCount++;
                }

                System.out.print("  Exception: " + lastError + " [" + lastErrorCount + " repeats]\r");
                continue;
            }

            lastError = "";
            counter++;

            if (failStart > 0) {
                long downTime = java.lang.System.currentTimeMillis() - failStart;
                failStart = 0;

                System.out.println("");
                System.out.print("Cluster is up after " + downTime + " ms: ");
            }

            if (counter == 1000) {
                System.out.print(".");
                counter = 0;
            }
        }
    }

    public static void main(String[] args)
    {
        CommandLineParser parser = new DefaultParser();
        Options options = prepareOptions();

        try {
            CommandLine commandLine = parser.parse(prepareOptions(), args);

            runTest(
                commandLine.getOptionValue(HOST),
                Integer.parseInt(commandLine.getOptionValue(PORT))
            );
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            new HelpFormatter().printHelp("jedietest", options);
        }
    }

    private static Options prepareOptions() {
        Options options = new Options();

        Option hostOption = Option.builder()
            .required()
            .longOpt(HOST)
            .hasArg()
            .type(String.class)
            .build();
        Option portOption = Option.builder()
            .required()
            .longOpt(PORT)
            .hasArg()
            .type(Number.class)
            .build();
        
        options.addOption(hostOption)
            .addOption(portOption);

        return options;
    }

}
