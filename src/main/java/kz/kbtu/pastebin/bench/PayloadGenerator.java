package kz.kbtu.pastebin.bench;

import java.util.Random;

/** Deterministic English-like text (words from a small list), so gzip ratios look like real notes/logs/code. */
final class PayloadGenerator {
    private static final String[] WORDS = ("the of and to in is that for it as with was on be by this are at from or have an not " +
            "but they which you one all were her she there their will each about how up out them then many some so these would " +
            "into has more two like him see time could no make than first been its who now people my made over did down only way " +
            "find use may water long little very after words called just where most know get through back much before go good new " +
            "write our me man too any day same right look think also around another came come work three word must because does " +
            "part even place well such here take why help put different away again off went old number great tell men say small " +
            "every found still between name should home big give air line set own under read last never us left end along while " +
            "might next sound below saw something thought both few those always looked show large often together asked house " +
            "world going want school important until form food keep children feet land side without boy once animal life enough " +
            "took sometimes four head above kind began almost live page got earth need far hand high year mother light country " +
            "father let night picture being study second soon story since white ever paper hard near sentence better best across " +
            "during today however sure knew try told young sun thing whole hear example heard several change answer room sea " +
            "against top turned learn point city play toward five using himself usually money seen didn't car morning").split(" ");

    private PayloadGenerator() {}

    /** Text of exactly `bytes` characters (ASCII, so also exactly `bytes` UTF-8 bytes). */
    static String text(int bytes, long seed) {
        Random r = new Random(seed);
        StringBuilder sb = new StringBuilder(bytes + 16);
        int lineLen = 0;
        while (sb.length() < bytes) {
            String w = WORDS[r.nextInt(WORDS.length)];
            sb.append(w);
            lineLen += w.length() + 1;
            if (lineLen > 70) {
                sb.append('\n');
                lineLen = 0;
            } else {
                sb.append(' ');
            }
        }
        sb.setLength(bytes);
        return sb.toString();
    }
}
