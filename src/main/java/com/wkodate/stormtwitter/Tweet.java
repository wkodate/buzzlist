package com.wkodate.stormtwitter;

/**
 * Tweeet.java
 *
 * Created by wkodate on 2015/05/24.
 */
public class Tweet {

    public final String screenName;
    public final String text;
    public final String createdAt;

    public Tweet(
            final String name,
            final String txt,
            final String date) {
        this.screenName = name;
        this.text = txt;
        this.createdAt = date;
    }
}
