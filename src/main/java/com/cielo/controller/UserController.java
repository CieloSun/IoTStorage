package com.cielo.controller;

import com.cielo.model.BodyWithToken;
import com.cielo.model.Permission;
import com.cielo.model.Role;
import com.cielo.model.User;
import com.cielo.service.UserService;
import com.cielo.ssdb.SSDBCommon;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.naming.AuthenticationException;
import javax.naming.NoPermissionException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@CrossOrigin
@RequestMapping("user")
public class UserController {
    @Autowired
    private SSDBCommon ssdbCommon;
    @Autowired
    private UserService userService;

    @PostMapping("user")
    public String setUp(String username, String password) throws Exception {
        if (ssdbCommon.exists(User.key(username))) throw new AuthenticationException("Username has existed.");
        User user = new User(username, password, Role.GUEST);
        ssdbCommon.setObject(User.key(username), user);
        return userService.generateToken(user);
    }

    @PostMapping("token")
    public String login(String username, String password) throws Exception {
        if (ssdbCommon.exists(User.key(username))) {
            User user = ssdbCommon.getObject(User.key(username), User.class);
            if (user.getPassword().equals(password)) return userService.generateToken(user);
            throw new AuthenticationException("Your password has error.");
        } else throw new AuthenticationException("Username does not exist.");
    }

    @DeleteMapping("token")
    public void logout(String token) {
        ssdbCommon.del(token);
    }

    @GetMapping("token")
    public User getUserInfo(String token) throws Exception {
        if (ssdbCommon.exists(token)) return ssdbCommon.getObject(token, User.class);
        else throw new AuthenticationException("Your token does not exists.");
    }

    @PutMapping("token")
    public String updateUser(String token, String password) throws Exception {
        User user = getUserInfo(token);
        ssdbCommon.del(token);
        user.setPassword(password);
        ssdbCommon.setObject(User.key(user.getUsername()), user);
        return userService.generateToken(user);
    }

    @GetMapping("role")
    public Role getRole(String token) throws Exception {
        User user = getUserInfo(token);
        return ssdbCommon.getObject(Role.key(user.getRoleId()), Role.class);
    }

    @GetMapping("permission")
    public Set<Permission> showPermission(String token) throws Exception {
        return getRole(token).getPermissions().parallelStream().map(permissionId -> ssdbCommon.getObject(Permission.key(permissionId), Permission.class)).collect(Collectors.toSet());
    }

    @PostMapping("admin/user")
    public void editUser(@RequestBody BodyWithToken<User> body) throws Exception {
        if (userService.hasPermission(getRole(body.getToken()), Permission.EDIT_USER)) {
            User user = body.getBody();
            ssdbCommon.setObject(User.key(user.getUsername()), user);
        } else throw new NoPermissionException("You do not have the permission for adding user.");
    }

    @GetMapping("admin/permission")
    public List<Permission> showAllPermission(String token) throws Exception {
        if (userService.hasPermission(getRole(token), Permission.GET_USER))
            return ssdbCommon.getArrayObject("permission_", Permission.class);
        else throw new NoPermissionException("You do not have the permission to show user.");
    }

    @PostMapping("admin/permission")
    public void addPermission(String token, Set<Integer> permissions) throws Exception {
        if (userService.hasPermission(getRole(token), Permission.EDIT_PERMISSION)) {
            Role role = new Role(ssdbCommon.count("role_"), permissions);
            ssdbCommon.setObject(Role.key(role.getRoleId()), Role.class);
        } else throw new NoPermissionException("You do not have the permission to edit permission.");
    }

    @PutMapping("admin/permission")
    public void editPermission(String token, Integer roleId, Set<Integer> permissions) throws Exception {
        if (userService.hasPermission(getRole(token), Permission.EDIT_PERMISSION)) {
            Role role = ssdbCommon.getObject(Role.key(roleId), Role.class);
            role.setPermissions(permissions);
            ssdbCommon.setObject(Role.key(roleId), Role.class);
        }
    }

}
