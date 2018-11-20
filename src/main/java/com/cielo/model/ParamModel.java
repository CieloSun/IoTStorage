package com.cielo.model;

import lombok.Data;

@Data
public class ParamModel<T> {
    private String stringParam;
    private int intParam;
    private T complexParam;
}
