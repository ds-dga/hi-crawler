import schedule
import time
import arrow
from nectec import get_hospitals as nectec_hi
from wesafe import get_hospitals as wesafe_hi
from ped import get_hospitals as ped_hi
from daily_proc import daily_record


def update_all():
    s = time.time()
    now = arrow.get().to("Asia/Bangkok")
    print(f"[{(time.time() - s):10.3f}] working...{now.isoformat()}")
    try:
        ped_hi()
        print(f"[{(time.time() - s):10.3f}] > PED done...")
    except Exception as e:
        print(f"[{(time.time() - s):10.3f}] PED Error...{e}")
    try:
        wesafe_hi()
        print(f"[{(time.time() - s):10.3f}] > WeSAFE done...")
    except Exception as e:
        print(f"[{(time.time() - s):10.3f}] WeSAFE Error...{e}")
    try:
        nectec_hi()
        print(f"[{(time.time() - s):10.3f}] > AMED done...")
    except Exception as e:
        print(f"[{(time.time() - s):10.3f}] AMED Error...{e}")


# schedule.every().day.at("05:35").do(update_all)
# schedule.every().day.at("08:35").do(update_all)
# schedule.every().day.at("10:35").do(update_all)
# schedule.every().day.at("13:35").do(update_all)
# schedule.every().day.at("16:35").do(update_all)
# schedule.every().day.at("20:35").do(update_all)
# schedule.every().day.at("22:35").do(update_all)
schedule.every().hour.at(":35").do(update_all)
schedule.every().day.at("18:50").do(daily_record)



while True:
    schedule.run_pending()
    time.sleep(1)
