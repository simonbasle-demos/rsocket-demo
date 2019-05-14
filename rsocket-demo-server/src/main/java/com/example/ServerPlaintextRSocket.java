package com.example;

import java.nio.charset.Charset;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

import io.netty.buffer.ByteBuf;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Simon Baslé
 */
class ServerPlaintextRSocket extends AbstractRSocket {

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        System.out.println("routing " + payload.getDataUtf8() + " to /dev/null");
        return Mono.fromRunnable(payload::release);
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return Mono.just(DefaultPayload.create(payload.getDataUtf8() + " pong"));
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        ByteBuf buf = payload.sliceData();
        int count = buf.readInt();
        int interval = buf.readInt();
        String unitEncoded = String.valueOf(buf.readCharSequence(buf.readableBytes(), Charset.defaultCharset()));
        ChronoUnit unit = ChronoUnit.valueOf(unitEncoded);
        payload.release();

        return Flux.interval(Duration.of(interval, unit))
                   .take(count)
                   .map(i -> i % 2 == 0 ? DefaultPayload.create("tick")
                           : DefaultPayload.create("tock"));
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return Flux.from(payloads)
                   .switchMap(payload -> {
                       ByteBuf buf = payload.sliceData();
                       int count = buf.readInt();
                       int interval = buf.readInt();
                       String unitEncoded = String.valueOf(buf.readCharSequence(buf.readableBytes(), Charset.defaultCharset()));
                       ChronoUnit unit = ChronoUnit.valueOf(unitEncoded);
                       payload.release();

                       if (count == 0 && interval == 0) {
                           Payload stopPayload = DefaultPayload.create("STOPPING");
                           return Mono.just(stopPayload);
                       }

                       Payload triggerPayload = DefaultPayload.create(
                               count + "x every " + interval + " " + unitEncoded);

                       return Flux.interval(Duration.of(interval, unit))
                                  .take(count)
                                  .map(i -> {
                                      if (i % 2 == 0) {
                                          return DefaultPayload.create("tick");
                                      }
                                      return DefaultPayload.create("tock");
                                  })
                                  .startWith(triggerPayload);
                   });
    }

}
