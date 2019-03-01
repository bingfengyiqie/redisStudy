package com.learn.redis;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(6390);
        while (true) {
            Socket socket = serverSocket.accept();
            InputStream in = socket.getInputStream();
            System.out.println(in.read());
        }
    }
}
