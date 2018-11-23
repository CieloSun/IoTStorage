package com.cielo.controller;

import com.cielo.model.Permission;
import com.cielo.model.Role;
import com.cielo.model.User;
import com.cielo.service.UserService;
import com.cielo.storage.SSDBUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.naming.AuthenticationException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@CrossOrigin
@RequestMapping("user")
public class UserController {
    @Autowired
    private SSDBUtil ssdbUtil;
    @Autowired
    private UserService userService;

    @PostMapping
    public String setUp(String username, String password) throws Exception {
        if (ssdbUtil.exists(User.key(username))) throw new AuthenticationException("Username has existed.");
        User user = new User(username, password, Role.GUEST);
        ssdbUtil.set(User.key(username), user);
        return userService.generateToken(user);
    }

    @PostMapping("token")
    public String login(String username, String password) throws Exception {
        if (ssdbUtil.exists(User.key(username))) {
            User user = ssdbUtil.get(User.key(username), User.class);
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
        ssdbUtil.del(token);
        user.setPassword(password);
        ssdbUtil.set(User.key(user.getUsername()), user);
        return userService.generateToken(user);
    }

    @GetMapping("role/{token}")
    public Role getRole(@PathVariable String token) throws Exception {
        return userService.getRole(token);
    }

    @GetMapping("permission/{token}")
    public Set<Permission> showPermission(@PathVariable String token) throws Exception {
        return getRole(token).getPermissions().parallelStream().map(permissionId -> ssdbUtil.get(Permission.key(permissionId), Permission.class)).collect(Collectors.toSet());
    }

    @PostMapping("admin/{token}")
    public void editUser(@PathVariable String token, @RequestBody User user) throws Exception {
        userService.authPermission(token, Permission.EDIT_USER);
        ssdbUtil.set(User.key(user.getUsername()), user);
    }

    @GetMapping("admin/permission/{token}")
    public List<Permission> showAllPermission(@PathVariable String token) throws Exception {
        userService.authPermission(token, Permission.GET_USER);
        return ssdbUtil.getMapValues("permission_", Permission.class);
    }

    @PostMapping("admin/role/{token}")
    public void addRole(@PathVariable String token, @RequestBody Set<Integer> permissions) throws Exception {
        userService.authPermission(token, Permission.EDIT_ROLE);
        Role role = new Role(ssdbUtil.count("role_"), permissions);
        ssdbUtil.set(Role.key(role.getRoleId()), role);
    }

    @PutMapping("admin/role/{roleId}/{token}")
    public void editRole(@PathVariable String token, @PathVariable Integer roleId, @RequestBody Set<Integer> permissions) throws Exception {
        userService.authPermission(token, Permission.EDIT_ROLE);
        Role role = ssdbUtil.get(Role.key(roleId), Role.class);
        role.setPermissions(permissions);
        ssdbUtil.set(Role.key(roleId), role);
    }

}
