package com.h2db.crudh2db.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
@Table(name = "device")
public class Device {
     @Id
     @GeneratedValue(strategy = GenerationType.IDENTITY)
     private long id;
     @Column(name = "device_name")
     private String device_name;
     @Column(name = "device_id")
     private long device_id;

}
