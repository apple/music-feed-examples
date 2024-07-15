/*
 * For licensing see accompanying LICENSE file.
 * Copyright (C) 2024 Apple Inc. All Rights Reserved.
 */

package com.apple.musicFeed.example;

import com.apple.musicFeed.example.models.Part;
import com.apple.musicFeed.example.models.PartResponse;
import com.apple.musicFeed.example.models.PartsException;
import com.apple.musicFeed.example.models.feed.Content;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.binary.Base64;
import org.apache.parquet.Strings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Command(name = "apple-music-feed-example", mixinStandardHelpOptions = true, version = "1.0",
        description = "Gets the parquet files for an example feed and stores the parquet files")
public class App implements Callable<Integer> {

    @Option(names = {"--key-id"}, description = "id of a key linked to your Apple Developer account")
    String keyId;

    @Option(names = {"--team-id"}, description = "id of your Apple Developer account team", required = true)
    String teamId;

    @Option(names = {"--secret-key-file-path"},
            description = "file path to your secret key. Should be in a .p8 format", required = true)
    Path secretKeyFilePath;

    @Option(names = {"--apple-music-feed-base-url"}, description = "url of Apple Music feed",
            defaultValue = "https://api.media.apple.com", required = true)
    String appleMediaFeedBaseUrl;

    @Option(names = {"--feed-name"}, description = "Feed name you wish to download", defaultValue = "artist",
            required = true)
    String feedName;

    @Option(names = {"--out-dir"}, description = "Directory where to store the parquet files", required = true)
    Path outDir;

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final Integer DAY_SECONDS = 24 * 60 * 60;
    private static final String FEED_PREFIX = "/v1/feed/";

    private final HttpClient client = HttpClient.newBuilder().build();


    @Override
    public Integer call() throws Exception {
        System.out.println("Sending requests to " + appleMediaFeedBaseUrl);
        String jwt = generateJwt(secretKeyFilePath, teamId, keyId);

        System.out.println("Getting the latest export for feed " + feedName);

        // get the latestExport id for the feed
        String latestExportId = getLatestExportId(jwt);

        // now get all of the URLs that we need to download the files
        List<Part> parts = getPartsForLatestExportId(latestExportId, jwt);

        //download the files into the out directory
        CompletableFuture[] futures = parts.stream().map(p -> CompletableFuture.runAsync(() -> {
            try {
                getAndWritePart(p);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        })).toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();

        // analyze the parquet files
        readParquet(outDir.toAbsolutePath().toString());

        return 0;
    }

    private String getLatestExportId(String jwt) throws URISyntaxException, IOException, InterruptedException {
        String latestUrl = appleMediaFeedBaseUrl + FEED_PREFIX + feedName + "/latest";
        HttpRequest request =
                HttpRequest.newBuilder().GET()
                        .uri(new URI(latestUrl)).header("Authorization", "Bearer " + jwt)
                        .build();
        Optional<InputStream> body = handleMediaAPIRequest(request, "latest export");
        return body.map(s -> {
            JsonNode node = null;
            try {
                node = MAPPER.readTree(s);
                JsonNode data = node.get("data");
                JsonNode first = data.get(0);
                return first.get("id").asText();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).orElseThrow(() -> new IllegalArgumentException("No latest export found for feed "));
    }

    private List<Part> getPartsForLatestExportId(final String latestExportId, final String jwt)
            throws URISyntaxException, IOException, InterruptedException {
        int limit = 20;

        System.out.println("Getting the parquet file URLS from export " + latestExportId);
        String partsUrl = appleMediaFeedBaseUrl + FEED_PREFIX + "exports/" + latestExportId + "/parts?limit=" + limit;

        PartResponse partResponse = (getParts(partsUrl, jwt));
        ArrayList<Part> parts = new ArrayList<>(partResponse.resources.parts.values());
        String nextLink = partResponse.nextLink;

        System.out.println("Received " + parts.size() + " part urls at offset 0");
        while (!Strings.isNullOrEmpty(nextLink)) {
            var nextUrl = appleMediaFeedBaseUrl + nextLink;
            var offset = getOffsetFromUrl(nextUrl)
                    .orElseThrow(() -> new PartsException("Illegally formatted nextUrl " + nextUrl));
            PartResponse nextPartResponse = getParts(nextUrl, jwt);
            ArrayList<Part> nextParts = new ArrayList<>(nextPartResponse.resources.parts.values());
            nextLink = partResponse.nextLink;
            System.out.println("Received " + nextParts.size() + " part urls at offset " + offset);
            parts.addAll(nextParts);
        }
        System.out.println("Found " + parts.size() + " total");
        return parts;
    }

    private void getAndWritePart(Part p) throws IOException {
        String url = p.attributes.exportLocation;
        Path fileName = outDir.resolve(p.id);
        ReadableByteChannel readableByteChannel = null;
        readableByteChannel = Channels.newChannel(new URL(url).openStream());
        try (FileOutputStream fileOutputStream = new FileOutputStream(String.valueOf(fileName))) {
            FileChannel fileChannel = fileOutputStream.getChannel();
            // transfer bytes without copying them into the buffer
            fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
        }
    }

    private PartResponse getParts(final String url, final String jwt)
            throws URISyntaxException, IOException, InterruptedException {
        HttpRequest request =
                HttpRequest.newBuilder().GET()
                        .uri(new URI(url))
                        .header("Authorization", "Bearer " + jwt)
                        .build();
        Optional<InputStream> body = handleMediaAPIRequest(request, "parts");
        return body.map(s -> {
            PartResponse partResponse;
            try {
                partResponse = MAPPER.readValue(s, PartResponse.class);
                return partResponse;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).orElseThrow(() -> new PartsException("No parts found for url " + url));
    }

    private String generateJwt(Path secretKeyFilePath, String teamId, String keyId)
            throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        long currentUnixSeconds = System.currentTimeMillis() / 1000L;
        String key;
        try {
            key = Files.readString(secretKeyFilePath);
        } catch (IOException e) {
            System.err.println("Unable to read secret key file: " + secretKeyFilePath);
            throw e;
        }

        byte[] keyBytes = Base64.decodeBase64(key.replace("-----BEGIN PRIVATE KEY-----\n", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replace("\n", ""));

        KeyFactory rsaKeyFactory = KeyFactory.getInstance("EC");
        PrivateKey privateKey = rsaKeyFactory.generatePrivate(new PKCS8EncodedKeySpec(keyBytes));

        HashMap<String, Object> headers = new HashMap<>();
        headers.put("kid", keyId);
        return JWT.create()
                .withClaim("exp", currentUnixSeconds + DAY_SECONDS)
                .withClaim("iss", teamId)
                .withClaim("iat", currentUnixSeconds)
                .withHeader(ImmutableMap.of("kid", keyId))
                .withHeader(headers)
                .sign(Algorithm.ECDSA256(null, (ECPrivateKey) privateKey));
    }

    private Optional<String> getOffsetFromUrl(String url) throws MalformedURLException {
        if (Strings.isNullOrEmpty(url)) {
            return Optional.empty();
        }
        List<String> paramStrings = List.of(new URL(url).getQuery().split("&"));
        for (String paramString : paramStrings) {
            if (paramString.contains("offset")) {
                return Optional.of(paramString.split("=")[1]);
            }
        }
        return Optional.empty();
    }

    private static void readParquet(String path) {

        // Create Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Parquet Reader")
                .config("spark.master", "local")
                .getOrCreate();

        // Read Parquet and encode it into a Java bean class
        Dataset<Content> df = spark.read().parquet(path).as(Encoders.bean(Content.class));

        // Do an explicit casting to avoid errors due to the Scala array being treated as type Object.
        Content[] contentItems = (Content[]) df.head(5);

        // Print out 5 content item names
        System.out.println("Here are 5 content items from this dataset:");
        for (Content content : contentItems) {
            String songString = String.format("Id: %s, name: %s", content.getId(), content.getNameDefault());
            System.out.println(songString);
        }
    }

    private Optional<InputStream> handleMediaAPIRequest(HttpRequest request, String resourceDescription)
            throws IOException, InterruptedException {
        HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        return switch (response.statusCode()) {
            case 401 -> throw new PartsException(
                    "Authentication Failed. Did you give the right team id, key id and p8 file?");
            case 429 -> throw new PartsException("Rate limited. Please try again.");
            case 404 -> Optional.empty();
            case 200 -> Optional.of(response.body());
            default -> {
                System.err.println(response.body());
                throw new PartsException(
                        "Incorrect Status code getting" + resourceDescription + ". Expected 200 or 404, received " +
                                response.statusCode());
            }
        };
    }

    public static void main(String[] args) {
        System.out.println("Hello!");
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }
}
