package com.cielo.demo.aspects;

import com.cielo.demo.service.core.UserService;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.naming.AuthenticationException;

@Aspect
@Service
@Order(1)
public class AuthAspect {
    @Autowired
    private UserService userService;

    @Before("execution(* com.cielo.demo.controller.DeviceController.*(String,..))")
    public void checkToken(JoinPoint joinPoint) throws AuthenticationException {
        userService.authToken(joinPoint.getArgs()[0].toString());
    }
}
