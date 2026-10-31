package kz.kbtu.pastebin.client;

public record GetResult(String key, String content, String createdAt, String expiresAt) {}
