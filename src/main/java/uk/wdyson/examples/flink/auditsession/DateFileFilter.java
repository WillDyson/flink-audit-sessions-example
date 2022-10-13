package uk.wdyson.examples.flink.auditsession;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.core.fs.Path;

public class DateFileFilter extends FilePathFilter {
    LocalDate minDate;

    DateFileFilter(String minDateStr) {
        minDate = LocalDate.parse(minDateStr, DateTimeFormatter.BASIC_ISO_DATE);
    }

    @Override
    public boolean filterPath(Path path) {
        // check default filter for filtered files
        if (createDefaultFilter().filterPath(path)) {
            return true;
        }

        try {
            // try and parse YYYYMMDD name
            LocalDate date = LocalDate.parse(path.getName(), DateTimeFormatter.BASIC_ISO_DATE);

            // date - minDate
            long daysDiff = Duration.between(minDate.atTime(0, 0), date.atTime(0, 0)).toDays();

            // filter directory if date less than minDate
            return daysDiff < 0;
        } catch (DateTimeParseException e) {
            // do not filter if it is not a YYYYMMDD directory
            return false;
        }
    }
}
