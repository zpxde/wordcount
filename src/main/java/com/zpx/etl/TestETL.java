package com.zpx.etl;

public class TestETL {

    public static void main(String[] args) {

        String check = "^((13[0-9])|(14[0-9])|(15[0-9])|(17[0-9])|(18[0-9]))\\d{8}$";

        String phone = "13522350013";

        System.out.println(phone.matches(check));

    }
}
