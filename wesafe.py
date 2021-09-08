from db import Database
import sys
import os
from json import loads
from requests import get
import arrow
from db import Database

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
    now = arrow.get().to("Asia/Bangkok").isoformat()
    db.insert_place_stats(
        medilo,
        now,
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
    )


def get_hospitals():
    url = f"{BASE_URL}/reports/domains/isolation"
    res = get(url, headers=get_headers())
    if res.status_code != 200:
        print(f"[{res.status_code}] {res.text}")
        return
    body = loads(res.text)
    if not body["success"]:
        print(f"[{res.status_code}] {res.text}")
        return

    db = Database()
    for rec in body["data"]:
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
            "WeSAFE",
        )
        push_mk2(db, rec, source="wesafe")


if __name__ == "__main__":
    if not TOKEN:
        print("No active WeSAFE TOKEN")
        sys.exit(0)

    get_hospitals()
