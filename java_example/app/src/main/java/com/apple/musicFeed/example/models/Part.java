/*
 * For licensing see accompanying LICENSE file.
 * Copyright (C) 2024 Apple Inc. All Rights Reserved.
 */

package com.apple.musicFeed.example.models;

public class Part {
    public String id;
    public PartAttributes attributes;

    String getId() {
        return id;
    }

    void setId(final String id) {
        this.id = id;
    }

    PartAttributes getAttributes() {
        return attributes;
    }

    void setAttributes(final PartAttributes attributes) {
        this.attributes = attributes;
    }

    public static class PartAttributes {
        public String exportLocation;

        String getExportLocation() {
            return exportLocation;
        }

        void setExportLocation(final String exportLocation) {
            this.exportLocation = exportLocation;
        }
    }
}
