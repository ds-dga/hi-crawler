from db import Database
import sys
import time
import os
from json import loads, dumps
from requests import get
import arrow
from db import Database
import uptime_pusher

TOKEN = os.getenv("BU_TOKEN", "")
BASE_URL = "https://api.aidery.io"


def get_headers():
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }


def push_mk2(db, rec, source="-"):
    medilo = db.get_or_create_medical_place(
        rec["hsMedicalUnitName"],
        rec["cdOrganizationMedicalUnit"],
        rec["emLocationType"],
        rec["crZoneCode"],
        rec["crProvinceCode"],
        rec["crAmpurCode"] if "crAmpurCode" in rec else "",
        rec["crTumbolCode"],
        rec["crBuildingName"],
        rec["statBedTotal"],
        source,
        None,
    )
    tmsp = arrow.get(rec["update_at"]).to("Asia/Bangkok")
    db.insert_place_stats(
        medilo,
        tmsp.isoformat(),
        rec["statBedTotal"],
        rec["statBedFree"],
        rec["patientWait"],
        rec["patientGreen"],
        rec["patientYellow"],
        rec["patientRed"],
        rec["emPatientFavipiravir"],
        rec["reportFlag"],
        rec["reportNote"],
        rec["statReportLink"],
        dumps(rec),
    )


def update_place_info(db, item):
    """
    [2021-09-21] weSafe เพิ่มข้อมูล "cdOrganizationMedicalUnit" : "KYD_02_11295" แล้วนะครับ (ผมใช้ KYD เพื่อให้เหมือนกับ RC ครับ) ส่วนใหญ่มีข้อมูลแล้วแต่ยังขาดอยู่อีกไม่กี่ที่ จะทยอยเติมให้ครบครับ

    This change causes multiple unique key to break because NULL is acceptable
    in unique keys... Well, first of we need to clean up multiple records
    """
    found = db.find_medical_place(item["hsMedicalUnitName"], "wesafe")
    if found:
        print(f"found - {found}")
        txt = f"""
        "code" = '{item["cdOrganizationMedicalUnit"]}',
        "location_type" = '{item["emLocationType"]}'
        WHERE "id" = '{found}'"""
        db.update_medical_place(txt)
        return
    print("not found - wait for next update to create a record")


def get_hospitals():
    url = f"{BASE_URL}/reports/domains/isolation"
    s = time.time()
    res = None
    try:
        res = get(url, headers=get_headers())
        uptime_pusher.push(res)
    except:
        duration = time.time() - s
        uptime_pusher.push_raw(url, 504, duration * 100)  # 504 gateway timeout
        return

    # --- end of uptime pusher ---
    if res.status_code != 200:
        print(f"[{res.status_code}] {res.text}")
        return
    body = loads(res.text)
    if not body["success"]:
        print(f"[{res.status_code}] {res.text}")
        return

    db = Database()
    for rec in body["data"]:
        # change all NULL or None in python to "" because NULL can break
        #   through unique keys
        if not rec["cdOrganizationMedicalUnit"]:
            rec["cdOrganizationMedicalUnit"] = ""

        tmsp = arrow.get(rec["update_at"]).to("Asia/Bangkok")
        db.insert_hospital(
            rec["cdOrganizationMedicalUnit"],
            rec["hsMedicalUnitName"],
            rec["emLocationType"],
            rec["crZoneCode"],
            rec["crProvinceCode"],
            "",
            rec["crTumbolCode"],
            None,
            None,
            rec["crBuildingName"],
            rec["statBedFree"],
            rec["statBedTotal"],
            rec["patientWait"],
            rec["patientGreen"],
            rec["patientYellow"],
            rec["patientRed"],
            rec["emPatientFavipiravir"],
            rec["statReportLink"],
            rec["reportFlag"],
            rec["reportNote"],
            tmsp.isoformat(),
            "wesafe",
        )
        push_mk2(db, rec, source="wesafe")


if __name__ == "__main__":
    if not TOKEN:
        print("No active WeSAFE TOKEN")
        sys.exit(0)

    get_hospitals()
