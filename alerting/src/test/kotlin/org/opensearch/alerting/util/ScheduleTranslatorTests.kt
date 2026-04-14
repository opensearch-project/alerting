package org.opensearch.alerting.util

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import org.opensearch.commons.alerting.model.CronSchedule
import org.opensearch.commons.alerting.model.IntervalSchedule
import java.time.ZoneId
import java.time.temporal.ChronoUnit

class ScheduleTranslatorTests {

    // --- IntervalSchedule tests ---

    @Test
    fun `translate 1 minute interval`() {
        val schedule = IntervalSchedule(1, ChronoUnit.MINUTES)
        val (expr, tz) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("rate(1 minute)", expr)
        assertNull(tz)
    }

    @Test
    fun `translate 5 minutes interval`() {
        val schedule = IntervalSchedule(5, ChronoUnit.MINUTES)
        val (expr, _) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("rate(5 minutes)", expr)
    }

    @Test
    fun `translate 1 hour interval`() {
        val schedule = IntervalSchedule(1, ChronoUnit.HOURS)
        val (expr, _) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("rate(1 hour)", expr)
    }

    @Test
    fun `translate 1 day interval`() {
        val schedule = IntervalSchedule(1, ChronoUnit.DAYS)
        val (expr, _) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("rate(1 day)", expr)
    }

    @Test
    fun `translate seconds interval converts to minutes`() {
        val schedule = IntervalSchedule(120, ChronoUnit.SECONDS)
        val (expr, _) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("rate(2 minutes)", expr)
    }

    @Test
    fun `translate seconds interval rounds up to 1 minute minimum`() {
        val schedule = IntervalSchedule(30, ChronoUnit.SECONDS)
        val (expr, _) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("rate(1 minute)", expr)
    }

    @Test
    fun `translate non-divisible seconds uses ceiling division`() {
        val schedule = IntervalSchedule(90, ChronoUnit.SECONDS)
        val (expr, _) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("rate(2 minutes)", expr)
    }

    // --- CronSchedule tests (from LLD table 5.3) ---

    @Test
    fun `translate every 5 hours cron`() {
        val schedule = CronSchedule("0 */5 * * *", ZoneId.of("UTC"))
        val (expr, tz) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("cron(0 */5 * * ? *)", expr)
        assertEquals(ZoneId.of("UTC"), tz)
    }

    @Test
    fun `translate weekday cron with dow range`() {
        // Unix: 0 0 * * 1-5 (Mon-Fri, 0=Sun)
        // EB: 0 0 ? * 2-6 * (Mon=2, Fri=6)
        val schedule = CronSchedule("0 0 * * 1-5", ZoneId.of("US/Pacific"))
        val (expr, tz) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("cron(0 0 ? * 2-6 *)", expr)
        assertEquals(ZoneId.of("US/Pacific"), tz)
    }

    @Test
    fun `translate daily at 2 30 AM`() {
        val schedule = CronSchedule("30 2 * * *", ZoneId.of("UTC"))
        val (expr, _) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("cron(30 2 * * ? *)", expr)
    }

    @Test
    fun `translate specific dom`() {
        // 1st of every month at midnight
        val schedule = CronSchedule("0 0 1 * *", ZoneId.of("UTC"))
        val (expr, _) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("cron(0 0 1 * ? *)", expr)
    }

    @Test
    fun `translate dow list`() {
        // Unix: 0 9 * * 0,6 (Sun and Sat)
        // EB: 0 9 ? * 1,7 *
        val schedule = CronSchedule("0 9 * * 0,6", ZoneId.of("UTC"))
        val (expr, _) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("cron(0 9 ? * 1,7 *)", expr)
    }

    @Test
    fun `translate Sunday as 0`() {
        val schedule = CronSchedule("0 0 * * 0", ZoneId.of("UTC"))
        val (expr, _) = ScheduleTranslator.toEventBridgeExpression(schedule)
        assertEquals("cron(0 0 ? * 1 *)", expr)
    }
}
