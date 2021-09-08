import psycopg2


class Database(object):
    """ """

    def __init__(self, dbconn=None):
        if dbconn is None:
            dbconn = "dbname='covid' user='sipp11' host='dga-vm1' port='35432' password='banshee10' sslmode='disable'"
        self.conn = psycopg2.connect(dbconn)
        self.cursor = self.conn.cursor()

    def create_table_trace(self):
        self.cursor.execute(
            """
        CREATE TABLE hospital (
            id uuid DEFAULT gen_random_uuid() NOT NULL,
            "cdOrganizationMedicalUnit" text NULL,
            "hsMedicalUnitName" text NOT NULL,
            "emLocationType" text NULL,
            "crZoneCode" text NULL,
            "crProvinceCode" text NULL,
            "crAmpurCode" text NULL,
            "crTumbolCode" text NULL,
            "crGeographicCoordinateLatitude" numeric NULL,
            "crGeographicCoordinateLongitude" numeric NULL,
            "crBuildingName" text NULL,
            "statBedFree" integer DEFAULT 0 NOT NULL,
            "statBedTotal" integer DEFAULT 0 NOT NULL,
            "patientWait" integer DEFAULT 0 NOT NULL,
            "patientGreen" integer DEFAULT 0 NOT NULL,
            "patientYellow" integer DEFAULT 0 NOT NULL,
            "patientRed" integer DEFAULT 0 NOT NULL,
            "emPatientFavipiravir" integer DEFAULT 0 NOT NULL,
            "statReportLink" text NULL,
            "reportFlag" integer DEFAULT 0 NOT NULL,
            "reportNote" text NOT NULL,
            source text NOT NULL,
            coords geometry,
            created_at timestamp with time zone DEFAULT now(),
            updated_at timestamp with time zone DEFAULT now() NOT NULL
        );

        ALTER TABLE ONLY public.hospital ADD CONSTRAINT hospital_pkey PRIMARY KEY (id);

        ALTER TABLE hospital ADD CONSTRAINT code_and_name_unique UNIQUE("cdOrganizationMedicalUnit", "hsMedicalUnitName");

        CREATE TRIGGER set_public_hospital_updated_at BEFORE UPDATE ON public.hospital FOR EACH ROW EXECUTE FUNCTION public.set_current_timestamp_updated_at();

        """
        )
        self.conn.commit()

    def get_cursor(self):
        return self.cursor

    def close(self):
        self.conn.close()

    def insert_hospital_bulk(self, data):
        items = ",".join(
            self.cursor.mogrify(
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_GeomFromText(%s, 4326))",
                x,
            ).decode("utf-8")
            for x in data
        )
        q = f"""INSERT INTO hospital ("cdOrganizationMedicalUnit","hsMedicalUnitName","emLocationType","crZoneCode","crProvinceCode","crAmpurCode","crTumbolCode","crGeographicCoordinateLatitude","crGeographicCoordinateLongitude","crBuildingName","statBedFree","statBedTotal","patientWait","patientGreen","patientYellow","patientRed","emPatientFavipiravir","statReportLink","reportFlag","reportNote","source","coords") VALUES"""
        try:
            self.cursor.execute(
                f"""{q} {items} ON CONFLICT ("cdOrganizationMedicalUnit", "hsMedicalUnitName") DO UPDATE SET ("statBedTotal", "patientWait", "patientGreen", "patientYellow", "patientRed", "emPatientFavipiravir", "reportFlag", "source") = (EXCLUDED."statBedTotal", EXCLUDED."patientWait", EXCLUDED."patientGreen", EXCLUDED."patientYellow", EXCLUDED."patientRed", EXCLUDED."emPatientFavipiravir", EXCLUDED."reportFlag", EXCLUDED."source")"""
            )
            self.conn.commit()
        except Exception as e:
            print(e, end="\r")
            self.conn.rollback()

    def get_or_create_medical_place(
        self,
        name,
        code,
        location_type,
        zone_code,
        province_code,
        ampur_code,
        tumbol_code,
        building_name,
        capacity,
        source,
        coords,
    ):
        q = f"SELECT DISTINCT id FROM medical_place WHERE name = '{name}' AND code = '{code}'"
        try:
            self.cursor.execute(q)
            one = self.cursor.fetchone()
            if one:
                # print("[get/create] got: ", one)
                return one[0]
        except Exception as e:
            print("[get/create] err: ", e)

        # insert and return id
        return self.insert_medical_place_bulk(
            [
                (
                    name,
                    code,
                    location_type,
                    zone_code,
                    province_code,
                    ampur_code,
                    tumbol_code,
                    building_name,
                    capacity,
                    source,
                    coords,
                )
            ]
        )

    def insert_medical_place_bulk(self, data):
        items = ",".join(
            self.cursor.mogrify(
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_GeomFromText(%s, 4326))",
                x,
            ).decode("utf-8")
            for x in data
        )
        q = f"""INSERT INTO medical_place ("name","code","location_type","zone_code","province_code","ampur_code","tumbol_code","building_name","capacity","source","coords") VALUES"""
        try:
            self.cursor.execute(
                f"""{q} {items} ON CONFLICT ("name", "code") DO UPDATE SET ("capacity", "building_name", "source") = (EXCLUDED."capacity", EXCLUDED."building_name", EXCLUDED."source") RETURNING id"""
            )
            self.conn.commit()
            return self.cursor.fetchone()[0]
        except Exception as e:
            print(e, end="\r")
            self.conn.rollback()
        return None

    def insert_hospital(
        self,
        cdOrganizationMedicalUnit,
        hsMedicalUnitName,
        emLocationType,
        crZoneCode,
        crProvinceCode,
        crAmpurCode,
        crTumbolCode,
        crGeographicCoordinateLatitude,
        crGeographicCoordinateLongitude,
        crBuildingName,
        statBedFree,
        statBedTotal,
        patientWait,
        patientGreen,
        patientYellow,
        patientRed,
        emPatientFavipiravir,
        statReportLink,
        reportFlag,
        reportNote,
        source="",
    ):
        coords = None
        if crGeographicCoordinateLatitude:
            coords = f"POINT({crGeographicCoordinateLongitude} {crGeographicCoordinateLatitude})"
        data = [
            (
                cdOrganizationMedicalUnit,
                hsMedicalUnitName,
                emLocationType,
                crZoneCode,
                crProvinceCode,
                crAmpurCode,
                crTumbolCode,
                crGeographicCoordinateLatitude,
                crGeographicCoordinateLongitude,
                crBuildingName,
                statBedFree,
                statBedTotal,
                patientWait,
                patientGreen,
                patientYellow,
                patientRed,
                emPatientFavipiravir,
                statReportLink,
                reportFlag,
                reportNote,
                source,
                coords,
            )
        ]
        self.insert_hospital_bulk(data)

    def insert_place_stats(
        self,
        place_at,
        timestamp,
        bed_total,
        bed_free,
        patient_wait,
        patient_green,
        patient_yellow,
        patient_red,
        patient_favipiravir,
        report_flag,
        report_note,
        report_link,
    ):
        data = [
            (
                place_at,
                timestamp,
                bed_total,
                bed_free,
                patient_wait,
                patient_green,
                patient_yellow,
                patient_red,
                patient_favipiravir,
                report_flag,
                report_note,
                report_link,
            )
        ]
        self.insert_place_stats_bulk(data)

    def insert_place_stats_bulk(self, data):
        items = ",".join(
            self.cursor.mogrify(
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                x,
            ).decode("utf-8")
            for x in data
        )
        q = f"""INSERT INTO place_stats ("place_at", "timestamp", "bed_total", "bed_free", "patient_wait", "patient_green", "patient_yellow", "patient_red", "patient_favipiravir", "report_flag", "report_note", "report_link") VALUES"""
        try:
            self.cursor.execute(
                f"""{q} {items} ON CONFLICT ("place_at", "timestamp") DO NOTHING"""
            )
            self.conn.commit()
        except Exception as e:
            print(e, end="\r")
            self.conn.rollback()
