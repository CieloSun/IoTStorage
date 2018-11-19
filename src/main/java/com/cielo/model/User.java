package com.cielo.model;

import lombok.Data;

@Data
public class User {
    public static String key(String username) {
        return "user_" + username;
    }

    public static String tokenKey(String token) {
        return "token_" + token;
    }

    private String username;
    private String password;
    private Integer roleId;

    public User() {
    }

    public User(String username, String password, Integer roleId) {
        this.username = username;
        this.password = password;
        this.roleId = roleId;
    }


}
