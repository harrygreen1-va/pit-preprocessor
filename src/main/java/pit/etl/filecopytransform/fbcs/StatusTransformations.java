package pit.etl.filecopytransform.fbcs;

import java.util.*;
import java.util.function.Function;

public class StatusTransformations {
    public static final String DENIED = "DENIED";
    public static final String REJECTED = "REJECTED";
    public static final String NO_ACTION = "NO_ACTION";
    public static final String VOID = "VOID";
    public static final String PAID = "PAID";
    public static final String INVALID = "INVALID";
    
    public static final String APPROVED = "APPROVED";
    public static final String BILL_SENT = "BILL_SENT";

    // Figure out claim status based on the line statuses
    public static final Map<String, Function<List<String>, String>> FUNCTIONS = new HashMap<>() {{
        put("invalid.status", s -> {
            if (!VALID_STATUSES.containsAll(s))
                return INVALID;
            if (s.contains(PAID))
                return PAID;
            final HashSet<String> unique = new HashSet<>(s);
            if (unique.contains(VOID) && unique.size() == 1)
                return VOID;
            if (unique.contains(NO_ACTION) && unique.size() == 1)
                return NO_ACTION;
            if (s.contains(REJECTED) || s.contains(DENIED))
                return REJECTED;
            return NO_ACTION;
        });
    }};

    public final static Set<String> VALID_STATUSES = new HashSet<>(Arrays.asList(
            DENIED,
            REJECTED,
            NO_ACTION,
            VOID,
            PAID,
            APPROVED,
            BILL_SENT
    ));
}