package com.netty.handler;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.io.IOException;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netty.bean.Person;
import com.netty.server.HttpKafkaServer;

public class HttpKafkaServerHandler extends SimpleChannelInboundHandler<Object> {

    private Producer<String, String> kafkaProducer;

    public HttpKafkaServerHandler(Producer<String, String> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
		// TODO Auto-generated method stub
		Gson GSON = new GsonBuilder().create();
		
		 if (msg instanceof HttpRequest) {
	            HttpRequest httpRequest = (HttpRequest) msg;
	            GSON.fromJson(msg.toString(), Person.class);
	            String uri = httpRequest.getUri();
	            KeyedMessage<String, String> message = new KeyedMessage<>("com.netty.handler", uri);
	            kafkaProducer.send(message);

	            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK);
	            ctx.write(response).addListener(ChannelFutureListener.CLOSE);
	        }

	}
}
