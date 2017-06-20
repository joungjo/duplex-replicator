package com.geovis.duplex.web;

import java.net.UnknownHostException;

import org.springframework.context.support.FileSystemXmlApplicationContext;

public class Main {
	public static void main(String[] args) throws UnknownHostException {
		new FileSystemXmlApplicationContext("/conf/jetty.xml");
	}
}
