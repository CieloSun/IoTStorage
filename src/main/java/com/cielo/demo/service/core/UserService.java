package com.cielo.demo.service.core;

import com.cielo.demo.model.user.Role;
import com.cielo.demo.model.user.User;
import com.cielo.storage.api.KVStoreUtil;
import com.cielo.storage.tool.StreamProxy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.naming.AuthenticationException;
import javax.naming.NoPermissionException;
import java.util.UUID;

@Service
public class UserService {
    @Autowired
    private KVStoreUtil KVStoreUtil;

    public String generateToken(User user) {
        String token = User.tokenKey(UUID.randomUUID().toString().replace("_", ""));
        KVStoreUtil.setx(token, user, 60 * 60 * 24);
        return token;
    }

    public User getUser(String token) throws AuthenticationException {
        authToken(token);
        KVStoreUtil.expire(token, 60 * 60 * 24);
        return KVStoreUtil.get(token, User.class);
    }

    public void authToken(String token) throws AuthenticationException {
        if (!KVStoreUtil.exists(token)) throw new AuthenticationException("Your stringParam does not authToken.");
    }

    public Role getRole(String token) throws AuthenticationException {
        return KVStoreUtil.get(Role.key(getUser(token).getRoleId()), Role.class);
    }

    public void deleteToken(String token) {
        KVStoreUtil.del(token);
    }

    public void authPermission(Role role, Integer permission) throws NoPermissionException {
        if (!StreamProxy.stream(role.getPermissions()).anyMatch(permissionId -> permissionId == permission))
            throw new NoPermissionException("You do not have the permission.");
    }

    public void authPermission(String token, Integer permission) throws NoPermissionException, AuthenticationException {
        authPermission(getRole(token), permission);
    }
}
