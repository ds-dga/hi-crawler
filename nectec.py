from json import loads
from requests import get
import arrow
from db import Database

BASE_URL = "https://datalake.ddc-care.com"


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
    db.insert_place_stats(
        medilo,
        rec["timestamp"],
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


def get_items(what):
    url = f"{BASE_URL}/api/object/{what}/data"
    res = get(url)
    if res.status_code != 200:
        print(f"[{res.status_code}] {res.text}")
        return
    body = loads(res.text)
    if body["count"] == 0:
        return []
    timestamp = body["update_ts"]
    tmsp = arrow.get(timestamp).to("Asia/Bangkok")
    data = [
        {
            "cdOrganizationMedicalUnit": i["hospcode"],
            "hsMedicalUnitName": i["hospname"],
            "emLocationType": i["hospital_type"].replace("null", ""),
            "crZoneCode": "",
            "crProvinceCode": i["prov_code"].replace("null", ""),
            "crAmpurCode": i["amp_code"].replace("null", ""),
            "crTumbolCode": i["tambon_code"].replace("null", ""),
            "crGeographicCoordinateLatitude": None,
            "crGeographicCoordinateLongitude": None,
            "crBuildingName": "",
            "statBedFree": i["bed_count"] - i["patient_active_count"],
            "statBedTotal": i["bed_count"],
            "patientWait": i["today_null_count"],
            "patientGreen": i["active_negative_count"],
            "patientYellow": i["active_positive_count"],
            "patientRed": i["active_urgent_count"],
            "emPatientFavipiravir": -1,
            "statReportLink": "",
            "reportFlag": -1,
            "reportNote": f"agency: {i['agency']}",
            "timestamp": tmsp.isoformat(),
        }
        for i in body["hstat"]
    ]
    return data


def get_hospitals():
    kinds = [
        "histats.hospitalnew.pub",
        "histats.dmscare.pub",
    ]
    result = []
    for k in kinds:
        result += get_items(k)

    db = Database()
    for rec in result:
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
            "nectec",
        )
        push_mk2(db, rec, source="nectec")


if __name__ == "__main__":
    get_hospitals()
