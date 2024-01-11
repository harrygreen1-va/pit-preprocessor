package pit.etl.dbloader.validation;

@FunctionalInterface
public interface ValueValidator {
    
    boolean isValid(String header, String val);

}
