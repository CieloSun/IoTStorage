package com.cielo.model;

import lombok.Data;

@Data
public class Permission {
    public static final int EDIT_ROLE = 0;
    public static final int GET_USER = 1;
    public static final int EDIT_USER = 2;
    public static final int SHOW_PERMISSION = 3;
    private Integer permissionId;
    private String remark;

    public Permission(Integer permissionId, String remark) {
        this.permissionId = permissionId;
        this.remark = remark;
    }

    public static String key(int permissionId) {
        return "permission_" + permissionId;
    }
}
