package com.cielo.controller;

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

    @PostMapping
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

    @DeleteMapping("token/{token}")
    public void logout(@PathVariable String token) {
        userService.deleteToken(token);
    }

    @GetMapping("{token}")
    public User getUser(@PathVariable String token) throws Exception {
        return userService.getUser(token);
    }

    @PutMapping("{token}")
    public String updateUser(@PathVariable String token, String password) throws Exception {
        User user = getUser(token);
        ssdbCommon.del(token);
        user.setPassword(password);
        ssdbCommon.setObject(User.key(user.getUsername()), user);
        return userService.generateToken(user);
    }

    @GetMapping("role/{token}")
    public Role getRole(@PathVariable String token) throws Exception {
        return userService.getRole(token);
    }

    @GetMapping("permission/{token}")
    public Set<Permission> showPermission(@PathVariable String token) throws Exception {
        return getRole(token).getPermissions().parallelStream().map(permissionId -> ssdbCommon.getObject(Permission.key(permissionId), Permission.class)).collect(Collectors.toSet());
    }

    @PostMapping("admin/{token}")
    public void editUser(@PathVariable String token,@RequestBody User user) throws Exception {
        if (userService.hasPermission(token, Permission.EDIT_USER)) {
            ssdbCommon.setObject(User.key(user.getUsername()), user);
        } else throw new NoPermissionException("You do not have the permission for adding user.");
    }

    @GetMapping("admin/permission/{token}")
    public List<Permission> showAllPermission(@PathVariable String token) throws Exception {
        if (userService.hasPermission(token, Permission.GET_USER))
            return ssdbCommon.getArrayObject("permission_", Permission.class);
        else throw new NoPermissionException("You do not have the permission to show user.");
    }

    @PostMapping("admin/role/{token}")
    public void addRole(@PathVariable String token, @RequestBody Set<Integer> permissions) throws Exception {
        if (userService.hasPermission(token, Permission.EDIT_ROLE)) {
            Role role = new Role(ssdbCommon.count("role_"), permissions);
            ssdbCommon.setObject(Role.key(role.getRoleId()), role);
        } else throw new NoPermissionException("You do not have the permission to edit permission.");
    }

    @PutMapping("admin/role/{roleId}/{token}")
    public void editRole(@PathVariable String token, @PathVariable Integer roleId, @RequestBody Set<Integer> permissions) throws Exception {
        if (userService.hasPermission(token, Permission.EDIT_ROLE)) {
            Role role = ssdbCommon.getObject(Role.key(roleId), Role.class);
            role.setPermissions(permissions);
            ssdbCommon.setObject(Role.key(roleId), role);
        }
    }

}
