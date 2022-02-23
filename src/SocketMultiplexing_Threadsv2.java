package io.socket_io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/*
    io模型：多路复用器（select/poll + epoll）

    服务端accept客户端连接, 客户端数据R/W，
        分别交给不同的多路复用器传递给内核，且由不同的线程来完成！！
 */
public class SocketMultiplexing_Threadsv2 {
    private ServerSocketChannel server = null;
    private Selector serverSelector = null;
    private Selector clientSelector1 = null;
    private Selector clientSelector2 = null;
    int port = 9090;

    public void serverInit() {
        try {
            //服务端socket
            server = ServerSocketChannel.open();
            //配置非阻塞地accept客户端连接
            server.configureBlocking(false);
            //服务端socket绑定端口，并监听该端口上是否有客户端连接
            server.bind(new InetSocketAddress(port));
            //server Selector: 负责accept客户端连接事件
            serverSelector = Selector.open();
            //client selector:轮流负责客户端数据读写事件
            clientSelector1 = Selector.open();
            clientSelector2 = Selector.open();

            //服务端socket注册到server selector上
            server.register(serverSelector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        NioThread t1, t2, t3;
        SocketMultiplexing_Threadsv2 service = new SocketMultiplexing_Threadsv2();
        service.serverInit();
        System.out.println("Server has started/服务端已启动....");
        t1 = new NioThread(service.serverSelector, 2); // Accept client's conn
        t2 = new NioThread(service.clientSelector1);    // Read/Write data from client-side
        t3 = new NioThread(service.clientSelector2);    // Read/Write data from client-side

        t1.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        t2.start();
        t3.start();
    }

}

class NioThread extends Thread {
    Selector selector;
    static int selectors = 0;
    int id = 0;

    //volatile static BlockingQueue<SocketChannel>[] queue;
    volatile static LinkedBlockingQueue<SocketChannel>[] queue;
    static AtomicInteger idx = new AtomicInteger();

    //该构造产生的NioThread，主要负责R/W客户端数据
    public NioThread(Selector selector) {
        this.selector = selector;
        id = idx.getAndIncrement() % selectors;
        System.out.printf("Worker Thread %s has started / 已启动...\n", id);
    }

    //该构造产生的NioThread（t1），主要负责accept客户端连接，
    //  并将新的客户端连接，轮流分配给负责读写的线程(t2, t3)，由它们去R/W客户端数据
    public NioThread(Selector selector, int n) {
        this.selector = selector;
        this.selectors = n;

        queue = new LinkedBlockingQueue[selectors];
        for (int i = 0; i < selectors; i++) {
            queue[i] = new LinkedBlockingQueue<SocketChannel>();
        }
        System.out.println("Server Thread[Boss] has started / 已启动...");
    }

    @Override
    public void run() {
        try {
            while (true) {
                //多路复用器selector识别到有IO状态变化的fd(注册在selector上)，通知用户程序处理
                while (selector.select(1000) > 0) {
                    Set<SelectionKey> keys = selector.selectedKeys();
                    Iterator<SelectionKey> iter = keys.iterator();
                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();
                        //移除内核fd返回区的fd（socket对应de文件描述符），避免后期该fd没有IO状态变化时，也会被重复读写
                        iter.remove();
                        if (key.isAcceptable()) {
                            //接收新的客户端连接
                            acceptHandler(key);
                        } else if(key.isReadable()) {
                            //读写客户端数据
                            readHandler(key);
                        }
                    }
                }

                //下面代码是给clientSelector1和clientSelector2去执行的！！！
                // 意思是，如果负责识别客户端IO状态的selector，发现自己身上没有注册的客户端(fd)/已注册的客户端没有数据，
                //  就先去register那些分配给自己的client(fd)
                if (!queue[id].isEmpty()) {
                    ByteBuffer buffer = ByteBuffer.allocate(8192);
                    SocketChannel client = queue[id].take();
                    client.register(selector, SelectionKey.OP_READ, buffer);
                    System.out.println("----------------------------");
                    System.out.printf("Latest client socket/新客户端 [%s] is dispatched at/被分配到 (%s)... \n",client.socket().getPort(), id);
                    System.out.println("----------------------------");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /*
        （服务端）接收客户端连接
     */
    private void acceptHandler(SelectionKey key) {
        SocketChannel client = null;
        ServerSocketChannel server = null;
        try {
            server = (ServerSocketChannel) key.channel();
            client = server.accept();
            client.configureBlocking(false);
            int num = idx.getAndIncrement() % selectors;
            queue[num].add(client);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
        （客户端）处理数据R/W
     */
    private void readHandler(SelectionKey key) {
        SocketChannel client = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();
        buffer.clear();
        int read = 0;
        try {
            while(true) {
                read = client.read(buffer);
                if (read > 0) { //客户端有数据输入
                    buffer.flip();
                    while (buffer.hasRemaining()) {
                        client.write(buffer);
                    }
                    buffer.clear(); //清除以读取的数据，避免重复读取
                }else if(read == 0) { //客户端暂时没有输入输入
                    //跳出循环，避免阻塞
                    break;
                } else { //read = -1，即客户端数据已读取完毕
                    //关闭当前客户端连接
                    client.close();
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
