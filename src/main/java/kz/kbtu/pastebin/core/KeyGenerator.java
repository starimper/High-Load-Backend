package kz.kbtu.pastebin.core;

import java.security.SecureRandom;

/** Generates the public snippet key: 8 random base62 characters from a cryptographic generator. */
public interface KeyGenerator {
    String next();

    KeyGenerator RANDOM_BASE62 = new KeyGenerator() {
        private static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        private final SecureRandom random = new SecureRandom();

        @Override
        public String next() {
            char[] c = new char[8];
            for (int i = 0; i < c.length; i++) {
                c[i] = ALPHABET.charAt(random.nextInt(ALPHABET.length()));
            }
            return new String(c);
        }
    };
}
