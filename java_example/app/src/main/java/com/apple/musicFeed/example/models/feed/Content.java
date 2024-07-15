/*
 * For licensing see accompanying LICENSE file.
 * Copyright (C) 2024 Apple Inc. All Rights Reserved.
 */

package com.apple.musicFeed.example.models.feed;

public class Content {

    private String id;
    private String nameDefault;

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setNameDefault(String nameDefault) {
        this.nameDefault = nameDefault;
    }

    public String getNameDefault() {
        return nameDefault;
    }
}
