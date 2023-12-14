package com.h2db.crudh2db;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
//@ImportResource("application-context.xml")
public class Crudh2dbApplication {

	public static void main(String[] args) {
		SpringApplication.run(Crudh2dbApplication.class, args);
	}

}
