package com.example;

import java.nio.charset.Charset;
import java.time.temporal.ChronoUnit;
import javax.swing.JOptionPane;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class ClientApp {

    private static RSocket json      = RSocketFactory.connect()
                                            .dataMimeType("json")
                                            .transport(TcpClientTransport.create("localhost",
                                         9090))
                                            .start()
                                            .block();
    private static RSocket plaintext = RSocketFactory.connect()
                                                 .dataMimeType("text/plain")
                                                 .transport(TcpClientTransport.create("localhost",
                                              9090))
                                                 .start()
                                                 .block();

    public static void main( String[] args ) {
        System.out.println("== FIRE AND FORGET ==");
        fireAndForget(json);
        fireAndForget(plaintext);

        System.out.println("== REQUEST-RESPONSE ==");
        requestResponse(json);
        requestResponse(plaintext);

        System.out.println("== REQUEST-STREAM ==");
        Payload countOfFive = countOf(5, 500, ChronoUnit.MILLIS);
        requestStream(json, countOfFive);
        requestStream(plaintext, countOfFive);
        countOfFive.release();

        System.out.println("== STREAM-STREAM ==");
        streamStream(json);
        streamStream(plaintext);
    }

    private static void fireAndForget(RSocket rSocket) {
        rSocket.fireAndForget(DefaultPayload.create("hello " + (rSocket == json ? "json" : "plaintext")))
               .block();
    }

    private static void requestResponse(RSocket rSocket) {
        String type = rSocket == json ? "json" : "plaintext";
        Payload payload = rSocket.requestResponse(DefaultPayload.create("ping"))
                                 .block();
        System.out.println(type + " request-response: " + payload.getDataUtf8());
        payload.release();
    }

    private static Payload countOf(int count, int interval, ChronoUnit unit) {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        buffer.writeInt(count)
              .writeInt(interval)
              .writeCharSequence(unit.name(), Charset.defaultCharset());
        return DefaultPayload.create(buffer);
    }

    private static void requestStream(RSocket rSocket, Payload payload) {
        final String type = rSocket == json ? "json" : "plaintext";
        rSocket.requestStream(payload)
                .map(pl-> {
                    String str = pl.getDataUtf8();
                    pl.release();
                    return str;
                })
                .doOnNext(v -> System.out.println(type + " request-stream " + v))
                .blockLast();
    }

    private static final String DEFAULT = "900 MILLIS";

    private static void streamStream(RSocket rSocket) {
        final Scheduler scheduler = Schedulers.newSingle("console");
        final String type = rSocket == json ? "json" : "plaintext";

        final Flux<Payload> flux = Flux.create(sink -> scheduler.schedule(() -> {
            String line;
            for(;;) {
                line = JOptionPane.showInputDialog("Choose next interval in " + type + ".\n" +
                        "Format is `Interval UNIT` (default `" + DEFAULT + "`): ");
                if (line == null || "STOP".equals(line)) {
                    break;
                } else if (line.isEmpty()) {
                    line = DEFAULT;
                }

                String[] instructions = line.split(" ");
                if (instructions.length != 2) {
                    break;
                }

                int interval;
                ChronoUnit unit;
                try {
                    interval = Integer.parseInt(instructions[0]);
                    unit = ChronoUnit.valueOf(instructions[1]);
                } catch (Throwable t) {
                    t.printStackTrace();
                    break;
                }

                Payload payload = countOf(100, interval, unit);

                sink.next(payload);
                payload.release();
            }

            Payload stopPayload = countOf(0, 0, ChronoUnit.SECONDS);
            sink.next(stopPayload);
            stopPayload.release();
            sink.complete();
        }));

        rSocket.requestChannel(flux)
               .map(pl-> {
                   String str = pl.getDataUtf8();
                   pl.release();
                   return str;
               })
               .doOnNext(v -> System.out.println(type + " stream-stream " + v))
               .blockLast();

        scheduler.dispose();
    }

}
