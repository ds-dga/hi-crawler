import sys
import os
from json import loads
from requests import get
import arrow
from db import Database

PED_APIKEY = os.getenv("PED_APIKEY", "")
BASE_URL = "https://ped-shelter-parse-eir4oz44ca-uk.a.run.app"


def get_headers():
    return {
        "Content-Type": "application/json",
        "x-api-key": PED_APIKEY,
    }


def push_mk2(db, rec):
    lon = rec["crGeographicCoordinateLongitude"]
    lat = rec["crGeographicCoordinateLatitude"]
    medilo = db.get_or_create_medical_place(
        rec["hsMedicalUnitName"],
        rec["cdOrganizationMedicalUnit"],
        rec["emLocationType"],
        rec["crZoneCode"],
        rec["crProvinceCode"],
        rec["crAmpurCode"],
        rec["crTumbolCode"],
        rec["crBuildingName"],
        rec["statBedTotal"],
        "ped",
        f"POINT({lon} {lat})",
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
    url = f"{BASE_URL}/pedthai/dashboard/hospitals"
    res = get(url, headers=get_headers())
    if res.status_code != 200:
        print(f"[{res.status_code}] {res.text}")
        return
    body = loads(res.text)
    if not body["success"]:
        print(f"[{res.status_code}] {res.text}")
        return

    db = Database()
    now = arrow.get().to("Asia/Bangkok").isoformat()
    for rec in body["data"]:
        db.insert_hospital(
            rec["cdOrganizationMedicalUnit"],
            rec["hsMedicalUnitName"],
            rec["emLocationType"],
            rec["crZoneCode"],
            rec["crProvinceCode"],
            rec["crAmpurCode"],
            rec["crTumbolCode"],
            rec["crGeographicCoordinateLatitude"],
            rec["crGeographicCoordinateLongitude"],
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
            now,
            "ped",
        )
        push_mk2(db, rec)


if __name__ == "__main__":
    if not PED_APIKEY:
        print("No active PED API KEY")
        sys.exit(0)

    get_hospitals()
