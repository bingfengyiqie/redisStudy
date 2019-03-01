package com.learn.redis;

import java.net.Socket;

public class Client {
    public static void main(String[] args) {
        Socket client;
        try {
            client = new Socket("192.168.56.102", 6379);
            client.getOutputStream().write('e');
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
