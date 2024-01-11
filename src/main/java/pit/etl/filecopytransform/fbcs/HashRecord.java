package pit.etl.filecopytransform.fbcs;

public record HashRecord(String claimId, String lineId, int rowNum, String rawFieldsStr, String hashValue) {
}