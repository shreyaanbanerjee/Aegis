package com.enterprise.aegis.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PiiMaskingService — The core privacy engine of the Aegis gateway.
 *
 * This service scans incoming text for Personally Identifiable Information (PII)
 * and replaces each detected entity with a structured placeholder token (e.g., [PERSON_1]).
 *
 * Detection Strategy (no external network calls):
 *  1. REGEX patterns for structured PII (emails, SSNs, phone numbers, IPs, credit cards, dates)
 *  2. Dictionary-based name detection using a curated list of common first names.
 *     (For production, replace with a locally bundled NLP model like OpenNLP's NER.)
 *
 * Token Format: [TYPE_INDEX] — e.g., [EMAIL_1], [PERSON_2], [SSN_1]
 * The returned MaskingResult contains both the masked text and the token-to-original mapping
 * which is then persisted to Redis by the TokenVaultService.
 *
 * Thread Safety: This service is stateless (no instance fields) and is safely
 * shared as a Spring singleton.
 */
@Slf4j
@Service
public class PiiMaskingService {

    // ─── Regex Patterns ───────────────────────────────────────────────────────

    /** Standard email address pattern. */
    private static final Pattern EMAIL_PATTERN = Pattern.compile(
            "[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * US SSN: supports 123-45-6789 and 123456789.
     * Uses word boundaries to avoid partial matches inside larger numbers.
     */
    private static final Pattern SSN_PATTERN = Pattern.compile(
            "\\b(?!000|666|9\\d{2})\\d{3}[-\\s]?(?!00)\\d{2}[-\\s]?(?!0000)\\d{4}\\b"
    );

    /** US phone numbers in various formats: (555) 123-4567, 555-123-4567, +15551234567 */
    private static final Pattern PHONE_PATTERN = Pattern.compile(
            "(?:(?:\\+?1\\s*(?:[.\\-]\\s*)?)?(?:\\(\\s*([2-9]\\d{2})\\s*\\)|([2-9]\\d{2}))\\s*(?:[.\\-]\\s*)?" +
            "([2-9]\\d{2})\\s*(?:[.\\-]\\s*)?([0-9]{4}))(?:\\s*(?:#|x\\.?|ext\\.?|extension)\\s*(\\d+))?"
    );

    /** IPv4 addresses. */
    private static final Pattern IP_PATTERN = Pattern.compile(
            "\\b(?:(?:25[0-5]|2[0-4]\\d|[01]?\\d\\d?)\\.){3}(?:25[0-5]|2[0-4]\\d|[01]?\\d\\d?)\\b"
    );

    /** Credit card numbers: 16-digit groups separated by spaces, dashes, or none. */
    private static final Pattern CREDIT_CARD_PATTERN = Pattern.compile(
            "\\b(?:4[0-9]{12}(?:[0-9]{3})?|" +           // Visa
            "5[1-5][0-9]{14}|" +                          // MasterCard
            "3[47][0-9]{13}|" +                           // American Express
            "3(?:0[0-5]|[68][0-9])[0-9]{11}|" +          // Diners Club
            "6(?:011|5[0-9]{2})[0-9]{12}|" +             // Discover
            "(?:2131|1800|35\\d{3})\\d{11})\\b"          // JCB
    );

    /** Common date formats: MM/DD/YYYY, DD-MM-YYYY, YYYY-MM-DD */
    private static final Pattern DATE_PATTERN = Pattern.compile(
            "\\b(?:(?:0?[1-9]|1[0-2])[/\\-](?:0?[1-9]|[12]\\d|3[01])[/\\-](?:19|20)\\d{2}|" +
            "(?:19|20)\\d{2}[/\\-](?:0?[1-9]|1[0-2])[/\\-](?:0?[1-9]|[12]\\d|3[01]))\\b"
    );

    // ─── Name Dictionary ─────────────────────────────────────────────────────

    /**
     * Curated list of common English first names.
     * In a production system, this would be loaded from a bundled resource file
     * or replaced by an OpenNLP PersonNameFinder backed by a local model file.
     */
    private static final Set<String> COMMON_FIRST_NAMES = new HashSet<>(Arrays.asList(
        "James", "John", "Robert", "Michael", "William", "David", "Richard", "Joseph",
        "Thomas", "Charles", "Christopher", "Daniel", "Matthew", "Anthony", "Mark",
        "Donald", "Steven", "Paul", "Andrew", "Joshua", "Kenneth", "Kevin", "Brian",
        "George", "Timothy", "Ronald", "Edward", "Jason", "Jeffrey", "Ryan",
        "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Elizabeth", "Susan",
        "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty", "Margaret", "Sandra",
        "Ashley", "Dorothy", "Kimberly", "Emily", "Donna", "Michelle", "Carol",
        "Amanda", "Melissa", "Deborah", "Stephanie", "Rebecca", "Sharon", "Laura",
        "Cynthia", "Kathleen", "Amy", "Angela", "Shirley", "Anna", "Brenda", "Pamela",
        // Common South Asian names for broader coverage
        "Rahul", "Priya", "Amit", "Neha", "Vikram", "Pooja", "Arun", "Deepa",
        "Sanjay", "Kavya", "Rohan", "Meera", "Arjun", "Sunita", "Rajesh", "Anita",
        "Shreya", "Ayaan", "Ishaan", "Diya", "Zara", "Aryan", "Ananya", "Rehan"
    ));

    // ─── Public API ──────────────────────────────────────────────────────────

    /**
     * Scans the input text and replaces all detected PII with structured tokens.
     *
     * @param inputText the raw text that may contain PII (from the user's prompt)
     * @return a MaskingResult containing the masked text and the token-to-original map
     */
    public MaskingResult mask(String inputText) {
        if (inputText == null || inputText.isBlank()) {
            return new MaskingResult(inputText, Collections.emptyMap());
        }

        // Map from placeholder token → original PII value
        Map<String, String> tokenMap = new LinkedHashMap<>();
        // Counters for each PII type (for deterministic, readable token names)
        Map<String, Integer> typeCounters = new HashMap<>();

        String maskedText = inputText;

        // Process regex-based patterns first (most reliable)
        maskedText = applyPattern(maskedText, EMAIL_PATTERN,     "EMAIL",   tokenMap, typeCounters);
        maskedText = applyPattern(maskedText, SSN_PATTERN,       "SSN",     tokenMap, typeCounters);
        maskedText = applyPattern(maskedText, CREDIT_CARD_PATTERN,"CC",     tokenMap, typeCounters);
        maskedText = applyPattern(maskedText, PHONE_PATTERN,     "PHONE",   tokenMap, typeCounters);
        maskedText = applyPattern(maskedText, IP_PATTERN,        "IP",      tokenMap, typeCounters);
        maskedText = applyPattern(maskedText, DATE_PATTERN,      "DATE",    tokenMap, typeCounters);

        // Dictionary-based name detection last (broader, slightly less precise)
        maskedText = applyNameDetection(maskedText, tokenMap, typeCounters);

        int piiCount = tokenMap.size();
        if (piiCount > 0) {
            log.info("PII masking complete: detected {} entities, types={}",
                    piiCount, typeCounters);
        } else {
            log.debug("PII masking: no PII detected in input");
        }

        return new MaskingResult(maskedText, tokenMap);
    }

    /**
     * Reverses a previously masked text by substituting all placeholder tokens
     * back to their original values using the provided token map.
     *
     * @param maskedText the text with [TYPE_N] placeholders
     * @param tokenMap   the map of placeholder → original value (from TokenVaultService)
     * @return the fully rehydrated text
     */
    public String rehydrate(String maskedText, Map<String, String> tokenMap) {
        if (maskedText == null || tokenMap == null || tokenMap.isEmpty()) {
            return maskedText;
        }
        String rehydrated = maskedText;
        for (Map.Entry<String, String> entry : tokenMap.entrySet()) {
            // Use Pattern.quote to safely escape any special chars in the token
            rehydrated = rehydrated.replace(entry.getKey(), entry.getValue());
        }
        log.debug("Rehydration complete: replaced {} tokens", tokenMap.size());
        return rehydrated;
    }

    // ─── Private Helpers ─────────────────────────────────────────────────────

    /**
     * Finds all matches of a regex pattern in the text, generates tokens for each,
     * records the mapping, and replaces the match with the token.
     */
    private String applyPattern(
            String text,
            Pattern pattern,
            String tokenType,
            Map<String, String> tokenMap,
            Map<String, Integer> typeCounters
    ) {
        StringBuffer result = new StringBuffer();
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            String match = matcher.group();
            // Check if we already have a token for this exact value (deduplication).
            String existingToken = findExistingToken(tokenMap, match);
            String token = existingToken != null
                    ? existingToken
                    : generateToken(tokenType, tokenMap, typeCounters, match);
            matcher.appendReplacement(result, Matcher.quoteReplacement(token));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    /**
     * Dictionary-based name detection.
     * Looks for words in the text that match known first names (case-sensitive,
     * word-boundary matched) and tokenizes them as PERSON entities.
     *
     * Heuristic: If a known first name is followed by a capitalized word
     * (potential surname), both are captured as a PERSON token.
     */
    private String applyNameDetection(
            String text,
            Map<String, String> tokenMap,
            Map<String, Integer> typeCounters
    ) {
        // Build a pattern dynamically from the name dictionary
        String namesAlt = String.join("|", COMMON_FIRST_NAMES);
        // Match: FirstName + optional SPACE + CapitalizedWord (surname)
        Pattern namePattern = Pattern.compile(
                "\\b(" + namesAlt + ")(?:\\s+[A-Z][a-z]+'?[a-z]*)?\\b"
        );

        StringBuffer result = new StringBuffer();
        Matcher matcher = namePattern.matcher(text);
        while (matcher.find()) {
            String fullMatch = matcher.group();
            // Skip if this span was already replaced by another token type
            if (fullMatch.startsWith("[") && fullMatch.endsWith("]")) {
                matcher.appendReplacement(result, Matcher.quoteReplacement(fullMatch));
                continue;
            }
            String existingToken = findExistingToken(tokenMap, fullMatch);
            String token = existingToken != null
                    ? existingToken
                    : generateToken("PERSON", tokenMap, typeCounters, fullMatch);
            matcher.appendReplacement(result, Matcher.quoteReplacement(token));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    /**
     * Generates a structured token like [EMAIL_1] and records the mapping.
     */
    private String generateToken(
            String tokenType,
            Map<String, String> tokenMap,
            Map<String, Integer> typeCounters,
            String originalValue
    ) {
        int index = typeCounters.merge(tokenType, 1, Integer::sum);
        String token = "[" + tokenType + "_" + index + "]";
        tokenMap.put(token, originalValue);
        log.debug("Masking '{}' → '{}'", originalValue, token);
        return token;
    }

    /**
     * Checks if the given original value already has a token (for deduplication).
     * Returns the existing token or null.
     */
    private String findExistingToken(Map<String, String> tokenMap, String value) {
        return tokenMap.entrySet().stream()
                .filter(e -> e.getValue().equals(value))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }

    // ─── Result Record ───────────────────────────────────────────────────────

    /**
     * Immutable result object returned by the mask() method.
     *
     * @param maskedText the PII-free text with structured placeholder tokens
     * @param tokenMap   map of [TOKEN] → original PII value for rehydration
     */
    public record MaskingResult(String maskedText, Map<String, String> tokenMap) {}
}
