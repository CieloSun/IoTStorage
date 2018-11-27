package com.cielo.service.core;

import com.cielo.model.user.Role;
import com.cielo.model.user.User;
import com.cielo.storage.core.SSDBUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.naming.AuthenticationException;
import javax.naming.NoPermissionException;
import java.util.UUID;

@Service
public class UserService {
    @Autowired
    private SSDBUtil ssdbUtil;

    public String generateToken(User user) {
        String token = User.tokenKey(UUID.randomUUID().toString().replace("_", ""));
        ssdbUtil.setx(token, user, 60 * 60 * 24);
        return token;
    }

    public User getUser(String token) throws AuthenticationException {
        authToken(token);
        ssdbUtil.expire(token, 60 * 60 * 24);
        return ssdbUtil.get(token, User.class);
    }

    public void authToken(String token) throws AuthenticationException {
        if (!ssdbUtil.exists(token)) throw new AuthenticationException("Your stringParam does not authToken.");
    }

    public Role getRole(String token) throws AuthenticationException {
        return ssdbUtil.get(Role.key(getUser(token).getRoleId()), Role.class);
    }

    public void deleteToken(String token) {
        ssdbUtil.del(token);
    }

    public void authPermission(Role role, Integer permission) throws NoPermissionException {
        if (!role.getPermissions().parallelStream().anyMatch(permissionId -> permissionId == permission))
            throw new NoPermissionException("You do not have the permission.");
    }

    public void authPermission(String token, Integer permission) throws NoPermissionException, AuthenticationException {
        authPermission(getRole(token), permission);
    }
}
