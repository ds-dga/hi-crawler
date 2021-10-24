from db import Database
import arrow

db = Database()


def get_days():
    q = "select distinct to_char(timestamp AT TIME ZONE 'Asia/Bangkok', 'yyyy-MM-dd') as day from place_stats order by day ASC;"
    cur = db.get_cursor()
    cur.execute(q)
    days = []
    for row in cur.fetchall():
        days.append(row[0])
    return days


def get_hospitals():
    q = "select id, name, source from medical_place;"
    cur = db.get_cursor()
    cur.execute(q)
    results = []
    for row in cur.fetchall():
        results.append(row)
    return results


def get_stats_on(hospital, day):
    q = f"""SELECT
            to_char(p.timestamp AT TIME ZONE 'Asia/Bangkok', 'HH24') as hr,
            p.bed_total, p.patient_wait,
            p.patient_green, p.patient_yellow, p.patient_red
        FROM place_stats p
        WHERE p.place_at = '{hospital[0]}'
            AND to_char(p.timestamp AT TIME ZONE 'Asia/Bangkok', 'yyyy-MM-dd') = '{day}'
        """
    cur = db.get_cursor()
    cur.execute(q)
    results = []
    hrs = []
    # print(f'========= {hospital[1]} ====================')
    for row in cur.fetchall():
        # print(row)
        results.append(row)
        hrs.append(int(row[0]))
    return hrs, results


def get_time_closet_to_18(hrs):
    if len(hrs) == 0:
        return None
    HR = 18
    if HR in hrs:
        return HR
    for i in range(1, 6):
        if (HR + i) in hrs:
            return HR + i
        if (HR - i) in hrs:
            return HR - i
    return hrs[-1]


def main():
    days = get_days()
    hospitals = get_hospitals()
    # print(days)
    # print(hospitals[:2])
    for hi in hospitals:
        print(f"========= {hi[1]} -- {hi[2]} ====================")
        for day in days:
            # skip today -- no 1800 data yet
            if day == "2021-10-06":
                continue
            hrs, items = get_stats_on(hi, day)
            tm = get_time_closet_to_18(hrs)
            # no data for this hospital on this day
            if tm is None:
                continue
            # print(hrs, tm)
            ind = hrs.index(tm)
            _h, bed_total, wait, green, yellow, red = items[ind]
            wait = None if wait < 0 else wait
            print(day, "T", tm, " = ", bed_total, wait, green, yellow, red)
            db.insert_daily_place_stats(hi[0], day, bed_total, wait, green, yellow, red)


def daily_record():
    today = arrow.get().to("Asia/Bangkok")
    hospitals = get_hospitals()
    q = """SELECT
            "timestamp", "bed_total",
            "patient_wait", "patient_green", "patient_yellow", "patient_red"
        FROM place_stats
        WHERE place_at = '{id}'
            AND to_char("timestamp" AT TIME ZONE 'Asia/Bangkok', 'yyyy-MM-dd') = '{day}'
        ORDER BY "timestamp" DESC
        LIMIT 1
        ;"""
    cur = db.get_cursor()
    cnt = 1
    for hi in hospitals:
        cur.execute(q.format(id=hi[0], day=today.format("YYYY-MM-DD")))
        row = cur.fetchone()
        if not row:
            continue
        tmsp = arrow.get(row[0]).to("Asia/Bangkok")
        # print(cnt, tmsp.isoformat(), row[1:])
        # log to daily_place_stats
        bed_total, wait, green, yellow, red = row[1:]
        db.insert_daily_place_stats(
            hi[0], tmsp.format("YYYY-MM-DD"), bed_total, wait, green, yellow, red
        )
        cnt += 1
    print(f"daily_place_stats: #{cnt} on {today.isoformat()}")
    # return results


if __name__ == "__main__":
    # main()
    daily_record()
