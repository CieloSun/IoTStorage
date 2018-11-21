package com.cielo.model;

import lombok.Data;

import java.util.Set;

@Data
public class Role {
    public static final int ADMIN = 0;
    public static final int GUEST = 1;
    private Integer roleId;
    private Set<Integer> permissions;
    public Role(Integer roleId, Set<Integer> permissions) {
        this.roleId = roleId;
        this.permissions = permissions;
    }

    public static String key(int roleId) {
        return "role_" + roleId;
    }
}
