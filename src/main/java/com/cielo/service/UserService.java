package com.cielo.service;

import com.cielo.model.Role;
import com.cielo.model.User;
import com.cielo.ssdb.SSDBCommon;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class UserService {
    @Autowired
    private SSDBCommon ssdbCommon;

    public String generateToken(User user) {
        String token = User.tokenKey(UUID.randomUUID().toString().replace("_", ""));
        ssdbCommon.setObjectWithTTL(token, user, 60 * 60 * 24);
        return token;
    }

    public User getByToken(String token) {
        return ssdbCommon.getObject(token, User.class);
    }

    public void deleteToken(String token) {
        ssdbCommon.del(token);
    }

    public boolean hasPermission(Role role, Integer permission) {
        return role.getPermissions().parallelStream().anyMatch(permissionId -> permissionId == permission);
    }
}
