const std = @import("std");

pub fn iso8601format(unix_us: i64, alloc: std.mem.Allocator) ![]u8 {
    const secs: i64 = @divFloor(unix_us, std.time.us_per_s);
    const millis: i64 = @mod(unix_us, std.time.us_per_ms);

    const es = std.time.epoch.EpochSeconds{ .secs = @intCast(secs) };
    const yd = es.getEpochDay().calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();

    return try std.fmt.allocPrint(
        alloc,
        "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}{d:0>3}Z",
        .{
            yd.year,
            md.month.numeric(),
            md.day_index + 1,
            ds.getHoursIntoDay(),
            ds.getMinutesIntoHour(),
            ds.getSecondsIntoMinute(),
            millis,
        },
    );
}
