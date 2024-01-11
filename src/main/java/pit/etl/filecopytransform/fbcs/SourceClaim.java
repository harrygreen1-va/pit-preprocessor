package pit.etl.filecopytransform.fbcs;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Getter()
@Accessors(fluent = true)
@ToString
@Slf4j
public class SourceClaim {

    private final String claimId;
    int claimLineNumber;

    @Getter(AccessLevel.NONE)
    private final Map<String, SourceLine> linesById = new LinkedHashMap<>();

    public SourceClaim(String claimId, SourceLine sourceLine) {
        this.claimId = claimId;
        claimLineNumber = sourceLine.lineNumber();
        linesById.put(sourceLine.lineIdOrKey(), sourceLine);
    }

    /**
     * Add the line, if there is already a line with the same line ID, keep the latest
     *
     * @param sourceLine line to add
     * @return duplicate line or null if no dupe
     */
    public SourceLine addSourceLineWithDupeDetection(SourceLine sourceLine) {
        SourceLine dupeLine = null;
        if (linesById.containsKey(sourceLine.lineIdOrKey())) {
            dupeLine = linesById.get(sourceLine.lineIdOrKey());
            log.info("Dupe: Line {} {} {} is a dupe of line {} {}. Keeping line {}",
                    dupeLine.lineIdOrKey(), dupeLine.lineNumber(), dupeLine.status(), sourceLine.lineNumber(), sourceLine.status(), sourceLine.lineNumber());
        }

        linesById.put(sourceLine.lineIdOrKey(), sourceLine);
        return dupeLine;
    }

    public List<SourceLine> sourceLines() {
        List<SourceLine> sourceLines = new ArrayList<>(linesById.values());
        sourceLines.sort(Comparator.comparing(SourceLine::lineNumber));
        return sourceLines;
    }

    public int lineCount() {
        return linesById.size();
    }

}