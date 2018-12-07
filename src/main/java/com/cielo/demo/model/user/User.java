package com.cielo.demo.model.user;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class User {
    private String username;
    private String password;
    private Integer roleId;
    private List<String> devices;

    public User() {
    }

    public User(String username, String password, Integer roleId) {
        this.username = username;
        this.password = password;
        this.roleId = roleId;
        this.devices = new ArrayList<>();
    }

    public static String key(String username) {
        return "user_" + username;
    }

    public static String tokenKey(String token) {
        return "token_" + token;
    }


}
