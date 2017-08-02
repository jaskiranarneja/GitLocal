package com.netty.initializer;

import com.netty.bean.Person;
import com.netty.handler.HttpKafkaServerHandler;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import kafka.javaapi.producer.Producer;

public class HttpKafkaInitializer extends ChannelInitializer<SocketChannel> {

    private Producer<String, String> kafkaProducer;

    public HttpKafkaInitializer(Producer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new HttpRequestDecoder());
        p.addLast(new HttpResponseEncoder());
        p.addLast(new HttpKafkaServerHandler(kafkaProducer));
        Person person = new Person();
        person.setFirstname("Jaskiran");
        person.setLastname("Arneja");
        HttpKafkaServerHandler handler = new HttpKafkaServerHandler(kafkaProducer);
        handler.acceptInboundMessage(person);
        System.out.println("Handler accept messages");
    }
}
