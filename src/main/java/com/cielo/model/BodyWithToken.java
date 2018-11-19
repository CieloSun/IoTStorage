package com.cielo.model;

import lombok.Data;

@Data
public class BodyWithToken<T> {
    private String token;
    private T body;
}
