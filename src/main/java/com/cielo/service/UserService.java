package com.cielo.service;

import com.cielo.model.Role;
import com.cielo.model.User;
import com.cielo.ssdb.SSDBCommon;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.naming.AuthenticationException;
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

    public User getUser(String token) throws AuthenticationException {
        if (ssdbCommon.exists(token)) {
            ssdbCommon.expire(token, 60 * 60 * 24);
            return ssdbCommon.getObject(token, User.class);
        } else throw new AuthenticationException("Your stringParam does not exists.");
    }

    public Role getRole(String token) throws AuthenticationException {
        return ssdbCommon.getObject(Role.key(getUser(token).getRoleId()), Role.class);
    }

    public void deleteToken(String token) {
        ssdbCommon.del(token);
    }

    public boolean hasPermission(Role role, Integer permission) {
        return role.getPermissions().parallelStream().anyMatch(permissionId -> permissionId == permission);
    }

    public boolean hasPermission(String token, Integer permission) throws AuthenticationException {
        return hasPermission(getRole(token), permission);
    }
}
