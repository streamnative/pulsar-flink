/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.pulsar.internal;

import lombok.val;
import lombok.var;
import org.apache.flink.api.java.tuple.Tuple2;

import javax.xml.bind.DatatypeConverter;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class DateTimeUtils {

    private static final long SECONDS_PER_DAY = 60 * 60 * 24L;
    private static final long MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L;

    // number of days between 1.1.1970 and 1.1.2001
    private static final int to2001 = -11323;
    private static final int toYearZero = to2001 + 7304850;

    // number of days in 400 years
    private static final int daysIn400Years = 146097;

    private static final long MICROS_PER_MILLIS = 1000L;
    private static final long MILLIS_PER_SECOND = 1000L;
    private static final long MICROS_PER_SECOND = MICROS_PER_MILLIS * MILLIS_PER_SECOND;

    public static Date stringToTime(String s) {
        val indexOfGMT = s.indexOf("GMT");
        if (indexOfGMT != -1) {
            // ISO8601 with a weird time zone specifier (2000-01-01T00:00GMT+01:00)
            val s0 = s.substring(0, indexOfGMT);
            val s1 = s.substring(indexOfGMT + 3);
            // Mapped to 2000-01-01T00:00+01:00
            return stringToTime(s0 + s1);
        } else if (!s.contains("T")) {
            // JDBC escape string
            if (s.contains(" ")) {
                return Timestamp.valueOf(s);
            } else {
                return java.sql.Date.valueOf(s);
            }
        } else {
            return DatatypeConverter.parseDateTime(s).getTime();
        }
    }

    /**
     * Returns the number of days since epoch from java.sql.Date.
     */
    public static int fromJavaDate(Date date) {
        return millisToDays(date.getTime());
    }

    // we should use the exact day as Int, for example, (year, month, day) -> day
    public static int millisToDays(long millisUtc) {
        return millisToDays(millisUtc, defaultTimeZone());
    }

    public static int millisToDays(long millisUtc, TimeZone timeZone) {
        val millisLocal = millisUtc + timeZone.getOffset(millisUtc);
        return (int) Math.floor((double) millisLocal / MILLIS_PER_DAY);
    }

    public static TimeZone defaultTimeZone() {
        return TimeZone.getDefault();
    }

    /**
     * Returns the number of micros since epoch from java.sql.Timestamp.
     */
    public static long fromJavaTimestamp(Timestamp t) {
        if (t != null) {
            return t.getTime() * 1000L + ((long) t.getNanos() / 1000) % 1000L;
        } else {
            return 0L;
        }
    }

    // Converts Timestamp to string according to Hive TimestampWritable convention.
    public static String timestampToString(long us, TimeZone timeZone) {
        val ts = toJavaTimestamp(us);
        val timestampString = ts.toString();
        val timestampFormat = getThreadLocalTimestampFormat(timeZone);
        val formatted = timestampFormat.format(ts);

        if (timestampString.length() > 19 && timestampString.substring(19) != ".0") {
            return formatted + timestampString.substring(19);
        } else {
            return formatted;
        }
    }

    // `SimpleDateFormat` is not thread-safe.
    private static ThreadLocal<DateFormat> threadLocalTimestampFormat =
        ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US));

    public static DateFormat getThreadLocalTimestampFormat(TimeZone timeZone) {
        val sdf = threadLocalTimestampFormat.get();
        sdf.setTimeZone(timeZone);
        return sdf;
    }

    /**
     * Returns a java.sql.Timestamp from number of micros since epoch.
     */
    public static Timestamp toJavaTimestamp(long us) {
        // setNanos() will overwrite the millisecond part, so the milliseconds should be
        // cut off at seconds
        var seconds = us / MICROS_PER_SECOND;
        var micros = us % MICROS_PER_SECOND;
        // setNanos() can not accept negative value
        if (micros < 0) {
            micros += MICROS_PER_SECOND;
            seconds -= 1;
        }
        val t = new Timestamp(seconds * 1000);
        t.setNanos((int) micros * 1000);
        return t;
    }

    /**
     * Returns a java.sql.Date from number of days since epoch.
     */
    public static Date toJavaDate(int daysSinceEpoch) {
        return new Date(daysToMillis(daysSinceEpoch));
    }

    // reverse of millisToDays
    public static long daysToMillis(int days) {
        return daysToMillis(days, defaultTimeZone());
    }

    public static long daysToMillis(int days, TimeZone timeZone) {
        val millisLocal = (long) days * MILLIS_PER_DAY;
        return millisLocal - getOffsetFromLocalMillis(millisLocal, timeZone);
    }

    /**
     * Lookup the offset for given millis seconds since 1970-01-01 00:00:00 in given timezone.
     * TODO: Improve handling of normalization differences.
     * TODO: Replace with JSR-310 or similar system
     */
    private static long getOffsetFromLocalMillis(long millisLocal, TimeZone tz) {
        var guess = tz.getRawOffset();
        // the actual offset should be calculated based on milliseconds in UTC
        val offset = tz.getOffset(millisLocal - guess);
        if (offset != guess) {
            guess = tz.getOffset(millisLocal - offset);
            if (guess != offset) {
                // fallback to do the reverse lookup using java.sql.Timestamp
                // this should only happen near the start or end of DST
                val days = (int) Math.floor((double) millisLocal / MILLIS_PER_DAY);
                val year = getYear(days);
                val month = getMonth(days);
                val day = getDayOfMonth(days);

                var millisOfDay = (int) (millisLocal % MILLIS_PER_DAY);
                if (millisOfDay < 0) {
                    millisOfDay += (int) MILLIS_PER_DAY;
                }
                val seconds = (int) (millisOfDay / 1000L);
                val hh = seconds / 3600;
                val mm = seconds / 60 % 60;
                val ss = seconds % 60;
                val ms = millisOfDay % 1000;
                val calendar = Calendar.getInstance(tz);
                calendar.set(year, month - 1, day, hh, mm, ss);
                calendar.set(Calendar.MILLISECOND, ms);
                guess = (int) (millisLocal - calendar.getTimeInMillis());
            }
        }
        return guess;
    }

    /**
     * Returns the year value for the given date. The date is expressed in days
     * since 1.1.1970.
     */
    public static int getYear(int date) {
        return getYearAndDayInYear(date).f0;
    }

    /**
     * Calculates the year and the number of the day in the year for the given
     * number of days. The given days is the number of days since 1.1.1970.
     *
     * The calculation uses the fact that the period 1.1.2001 until 31.12.2400 is
     * equals to the period 1.1.1601 until 31.12.2000.
     */
    private static Tuple2<Integer, Integer> getYearAndDayInYear(int daysSince1970) {
        // add the difference (in days) between 1.1.1970 and the artificial year 0 (-17999)
        var  daysSince1970Tmp = daysSince1970;
        // Since Julian calendar was replaced with the Gregorian calendar,
        // the 10 days after Oct. 4 were skipped.
        // (1582-10-04) -141428 days since 1970-01-01
        if (daysSince1970 <= -141428) {
            daysSince1970Tmp -= 10;
        }
        val daysNormalized = daysSince1970Tmp + toYearZero;
        val numOfQuarterCenturies = daysNormalized / daysIn400Years;
        val daysInThis400 = daysNormalized % daysIn400Years + 1;
        val yD = numYears(daysInThis400);
        int year = (2001 - 20000) + 400 * numOfQuarterCenturies + yD.f0;
        return Tuple2.of(year, yD.f1);
    }

    /**
     * Calculates the number of years for the given number of days. This depends
     * on a 400 year period.
     * @param days days since the beginning of the 400 year period
     * @return (number of year, days in year)
     */
    private static Tuple2<Integer, Integer> numYears(int days) {
        val year = days / 365;
        val boundary = yearBoundary(year);
        if (days > boundary) {
            return new Tuple2<>(year, days - boundary);
        } else {
            return new Tuple2<>(year - 1, days - yearBoundary(year - 1));
        }
    }

    /**
     * Return the number of days since the start of 400 year period.
     * The second year of a 400 year period (year 1) starts on day 365.
     */
    private static int yearBoundary(int year) {
        return year * 365 + ((year / 4 ) - (year / 100) + (year / 400));
    }

    /**
     * Returns the month value for the given date. The date is expressed in days
     * since 1.1.1970. January is month 1.
     */
    public static int getMonth(int date) {
        val entry = getYearAndDayInYear(date);
        var year = entry.f0;
        var dayInYear = entry.f1;

        if (isLeapYear(year)) {
            if (dayInYear == 60) {
                return 2;
            } else if (dayInYear > 60) {
                dayInYear = dayInYear - 1;
            }
        }

        if (dayInYear <= 31) {
            return 1;
        } else if (dayInYear <= 59) {
            return 2;
        } else if (dayInYear <= 90) {
            return 3;
        } else if (dayInYear <= 120) {
            return 4;
        } else if (dayInYear <= 151) {
            return 5;
        } else if (dayInYear <= 181) {
            return 6;
        } else if (dayInYear <= 212) {
            return 7;
        } else if (dayInYear <= 243) {
            return 8;
        } else if (dayInYear <= 273) {
            return 9;
        } else if (dayInYear <= 304) {
            return 10;
        } else if (dayInYear <= 334) {
            return 11;
        } else {
            return 12;
        }
    }

    /**
     * Returns the 'day of month' value for the given date. The date is expressed in days
     * since 1.1.1970.
     */
    public static int getDayOfMonth(int date) {
        val entry = getYearAndDayInYear(date);
        var year = entry.f0;
        var dayInYear = entry.f1;

        if (isLeapYear(year)) {
            if (dayInYear == 60) {
                return 29;
            } else if (dayInYear > 60) {
                dayInYear = dayInYear - 1;
            }
        }

        if (dayInYear <= 31) {
            return dayInYear;
        } else if (dayInYear <= 59) {
            return dayInYear - 31;
        } else if (dayInYear <= 90) {
            return dayInYear - 59;
        } else if (dayInYear <= 120) {
            return dayInYear - 90;
        } else if (dayInYear <= 151) {
            return dayInYear - 120;
        } else if (dayInYear <= 181) {
            return dayInYear - 151;
        } else if (dayInYear <= 212) {
            return dayInYear - 181;
        } else if (dayInYear <= 243) {
            return dayInYear - 212;
        } else if (dayInYear <= 273) {
            return dayInYear - 243;
        } else if (dayInYear <= 304) {
            return dayInYear - 273;
        } else if (dayInYear <= 334) {
            return dayInYear - 304;
        } else {
            return dayInYear - 334;
        }
    }

    private static boolean isLeapYear(int year) {
        return (year % 4) == 0 && ((year % 100) != 0 || (year % 400) == 0);
    }
}
