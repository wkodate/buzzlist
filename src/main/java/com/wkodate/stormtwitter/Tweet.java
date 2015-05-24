package com.wkodate.stormtwitter;

/**
 * Tweeet.java
 *
 * Created by wkodate on 2015/05/24.
 */
public class Tweet {

    public String screenName;
    public String text;
    public String createdAt;

    public Tweet(String name, String text, String date) {
        this.screenName = name;
        this.text = text;
        this.createdAt = date;
    }
}
