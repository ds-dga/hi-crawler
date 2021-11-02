from json import loads, dumps
import time
from requests import get
import arrow
from db import Database
import uptime_pusher

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
        rec["extras"],
    )


def get_items(what):
    url = f"{BASE_URL}/api/object/{what}/data"
    s = time.time()
    res = None
    try:
        res = get(url)
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
    if body["count"] == 0:
        return []
    timestamp = body["update_ts"]
    tmsp = arrow.get(timestamp).to("Asia/Bangkok")

    data = []
    for i in body["hstat"]:
        place_code = i["cdOrganizationMedicalUnit"]
        pcs = place_code.split("_")
        loc_type = i["hospital_type"].replace("null", "")
        if len(pcs) == 3:
            loc_type = pcs[1]
        # nectec ain't going to fix duplicate cdOrganizationMedicalUnit, then we do our own code which is cdOrganizationMedicalUnit concats with nectec's hospcode to make sure if record is identifiable.
        place_code = "_".join([place_code, i["hospcode"]])
        data.append(
            {
                "cdOrganizationMedicalUnit": place_code,
                "hsMedicalUnitName": i["hospname"],
                "emLocationType": loc_type,
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
                "extras": dumps(i),
            }
        )

    return data


def update_place_info(db, item):
    """
    [2021-09-18] AMED เพิ่มฟิลด์ "cdOrganizationMedicalUnit": "AMED_02_42546"

    ใน API เรียบร้อยแล้ว ทั้งฝั่ง กทม. และ กรมการแพทย์
    https://datalake.ddc-care.com/api/object/histats.hospitalnew.pub/data
    https://datalake.ddc-care.com/api/object/histats.dmscare.pub/data

    As a result, we have both new "code" and "location_type" with "name" remains the same.

    Thus, find by "name", then update "code" and "location_type" accordingly.
    """
    found = db.find_medical_place(item["hsMedicalUnitName"], "nectec")
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
    kinds = [
        "histats.hospitalnew.pub",
        "histats.dmscare.pub",
    ]
    result = []
    for k in kinds:
        try:
            result += get_items(k)
        except:
            pass

    db = Database()
    for rec in result:
        # update_place_info(db, rec)
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
            rec["timestamp"],
            "nectec",
        )
        push_mk2(db, rec, source="nectec")


if __name__ == "__main__":
    get_hospitals()
