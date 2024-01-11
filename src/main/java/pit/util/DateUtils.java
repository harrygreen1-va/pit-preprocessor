package pit.util;

import org.apache.commons.lang3.time.DateParser;
import org.apache.commons.lang3.time.FastDateFormat;
import pit.UnrecoverableException;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

@SuppressWarnings({"unused", "WeakerAccess"})
public class DateUtils {

    // ISO format as dictated by JSON convention, with offset from UTC
    public final static FastDateFormat ISO_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    public static String dateToIsoStr(Date date) {
        return ISO_FORMAT.format(date);
    }

    public static boolean isBefore(Date date1, Date date2) {
        LocalDate ld1 = date1.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
        LocalDate ld2 = date2.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();

        return ld1.isBefore(ld2);
    }

    public static boolean isWithinLastNMinutes(Date date, int minutes) {
        Date now = new Date();
        Date cutoff = org.apache.commons.lang3.time.DateUtils.addMinutes(now, -minutes);
        return date.after(cutoff);
    }

    public final static FastDateFormat YEAR_MONTH_DAY_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd");

    public static Date fromYMD(String dateStr) {
        try {
            return YEAR_MONTH_DAY_FORMAT.parse(dateStr);
            // TODO: may be replace with returning an empty date and a warning?
        } catch (ParseException e) {
            throw new UnrecoverableException(e);
        }

    }

    public final static FastDateFormat DD_MMM_YY_FORMAT = FastDateFormat.getInstance("dd-MMM-yy");

    public static Date fromDDMMMYY(String dateStr) {
        try {
            return DD_MMM_YY_FORMAT.parse(dateStr);
            // TODO: may be replace with returning an empty date and a warning?
        } catch (ParseException e) {
            throw new UnrecoverableException(e);
        }
    }

    public final static FastDateFormat YYYY_MM_DD_FORMAT = FastDateFormat.getInstance("yyyyMMdd");

    public static Date fromYMDNoDelim(String dateStr) {
        try {
            return YYYY_MM_DD_FORMAT.parse(dateStr);
            // TODO: may be replace with returning an empty date and a warning?
        } catch (ParseException e) {
            throw new UnrecoverableException(e);
        }
    }
    /**
     * @param targetDate The variable date in question
     * @param referenceDate The starting date to calculate time period
     * @param period The period of days to determine the date range 
     * @return True if targetDate is within period number of days from referenceDate
     * period can be a positive integer to test for dates in the past. If target date fell in the specified range.
     * period can be a negative integer to test for future dates, if target date will fall in the specified range.
     * period can be 0 to test for same day.
     */
    @SuppressWarnings("unused")
    public static boolean isWithinRangeOfTargetDate(LocalDate targetDate, LocalDate referenceDate, Long period) {
        if (targetDate==null || referenceDate ==null) {
            return false;
        }
        
        boolean isWithinRange;
        if (period > 0) {
            isWithinRange = targetDate.isAfter(referenceDate.minusDays(period+1));//+1 to be date inclusive
        } else if (period < 0) {
            isWithinRange = targetDate.isBefore(referenceDate.plusDays(period+1));//+1 to be date inclusive
        } 
        else {
            isWithinRange = targetDate.isEqual(referenceDate);
        }
        
        return isWithinRange;
    }
    
    public static Date fromString(String dateString, String format) {
        DateParser DOB_PARSER = FastDateFormat.getInstance(format);
        
        Date date;
        try {
            date = DOB_PARSER.parse(dateString);
        } catch (ParseException e) {
            throw new UnrecoverableException(e);
        }

        return date;
    }

}
