package com.example;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.TcpServerTransport;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

public class Server implements Disposable {

    private final Disposable server;
    private final CountDownLatch latch;

    public Server() {
        this.latch = new CountDownLatch(1);
        this.server = RSocketFactory
                .receive()
                .acceptor((setupPayload, reactiveSocket) -> byMimeType(setupPayload.dataMimeType()))
                .transport(TcpServerTransport.create("localhost", 9090))
                .start()
                .subscribe();
    }

    private Mono<RSocket> byMimeType(String dataMimeType) {
        if (dataMimeType.equals("json")) return Mono.just(new ServerJsonRSocket());
        return Mono.just(new ServerPlaintextRSocket());
    }

    public void await(Duration duration) {
        try {
            this.latch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void dispose() {
        latch.countDown();
        server.dispose();
    }

    @Override
    public boolean isDisposed() {
        return server.isDisposed() || latch.getCount() == 0;
    }

    public static void main(String[] args) {
        Server s = new Server();

        s.await(Duration.ofMinutes(10));
    }

}
