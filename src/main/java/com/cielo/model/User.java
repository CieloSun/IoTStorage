package com.cielo.model;

import lombok.Data;

@Data
public class User {
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

    public static String key(String username) {
        return "user_" + username;
    }

    public static String tokenKey(String token) {
        return "token_" + token;
    }


}
