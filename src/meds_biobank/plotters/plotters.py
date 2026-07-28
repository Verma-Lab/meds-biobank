import matplotlib.pyplot as plt
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from PIL import Image, ImageDraw, ImageFont

EVENT_EMOJIS = {
    "death": "\U0001F480",
    "visit_admission": "\U0001F3E5",
    "visit_discharge": "\U0001F6AA",
    "visit_flag": "\U0001F6A9",
    "drug": "\U0001F48A",
    "condition": "\U0001FA7A",
    "procedure": "\U0001F52A",
    "observation": "\U0001F441",
    "measurement": "\U0001F4C8",
}
DEFAULT_EMOJI = "\U000026AA"
EMOJI_FONT_PATH = "/System/Library/Fonts/Apple Color Emoji.ttc"
_emoji_image_cache = {}

def _emoji_image(emoji_char, size=160):
        if emoji_char in _emoji_image_cache:
            return _emoji_image_cache[emoji_char]
        font = ImageFont.truetype(EMOJI_FONT_PATH, size)
        img = Image.new("RGBA", (size, size), (255, 255, 255, 0))
        draw = ImageDraw.Draw(img)
        draw.text((0, 0), emoji_char, font=font, embedded_color=True)
        _emoji_image_cache[emoji_char] = img
        return img

def plot_events_emoji(events, concept, visit_id):
        """
        Args:
            events (List<Dict>):
                Desc: Records for a single patient, ordered by time, asc
                Dict Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|

        Notes:
            • 
        """
        events = [e for e in events if e["event_type"] not in {"race", "gender", "birth", "ethnicity"}]
        lv_types = {e["event_type"] for e in events if e["event_type"].startswith("labs_") or e["event_type"].startswith("vitals_")}
        fig, ax = plt.subplots(figsize=(18, 4))
        ax.hlines(y=0, xmin=events[0]["time"], xmax=events[-1]["time"])
        total_span = (events[-1]["time"] - events[0]["time"]).total_seconds()
        gap_threshold = total_span * 0.01
        last_time = None
        offset = 12
        direction = 1
        for e in events:
            label = concept[str(e.get("code"))]
            t = e["time"]
            event_type = e["event_type"]
            if event_type in EVENT_EMOJIS:
                emoji = EVENT_EMOJIS[event_type]
            elif event_type in lv_types:
                emoji = EVENT_EMOJIS["measurement"]
            else:
                emoji = DEFAULT_EMOJI
            imagebox = OffsetImage(_emoji_image(emoji), zoom=0.15)
            ax.add_artist(AnnotationBbox(imagebox, (t, 0), frameon=False, zorder=3))
            if last_time is not None and (t - last_time).total_seconds() < gap_threshold:
                offset += 10
                direction *= -1
            else:
                offset = 12
                direction = 1
            last_time = t
            ax.annotate(label + ".", (t, 0), xytext=(5, direction * offset), textcoords="offset points", fontsize=7, rotation=45)
        ax.set_yticks([])
        ax.set_ylim(-1, 1)
        ax.set_xlabel("time")
        ax.set_title(f"Patient {events[0].get('patient_id')} Visit {visit_id} timeline")
        fig.autofmt_xdate()
        plt.tight_layout()
        plt.show()

if __name__ == "__main__":

    # imports
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    import random

    # spark setup
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("meds-biobank-plotter")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    # load patient events and prepare as dict
    meds_events = spark.read.csv("/Users/zolensky/Code/meds-biobank/data/MEDS/pmbb_meds.csv", header=True, inferSchema=True)
    patient_ids = [row["patient_id"] for row in meds_events.select("patient_id").collect()]
    patient_id = random.choice(patient_ids)
    meds_events = meds_events.filter(F.col("patient_id") == patient_id)
    visit_ids = [row["visit_id"] for row in meds_events.select("visit_id").collect()]
    visit_id = random.choice(visit_ids)
    meds_events = meds_events.filter(F.col("visit_id") == visit_id)
    meds_events_dict = [row.asDict() for row in meds_events.collect()]
    print(f"Selected Patient: {patient_id}, Visit: {visit_id}")

    # load concept and prepare as dict
    CUSTOM_CONCEPTS = {
        "IsHospitalAdmission": 700000001,
        "IsInpatientAdmission": 700000002,
        "IsObservation": 700000003,
        "IsEdVisit": 700000004,
        "IsOutpatientFaceToFaceVisit": 700000005,
        "IsVideoVisit": 700000007,
    }
    CONCEPTS_CUSTOM = {str(v):k for k,v in CUSTOM_CONCEPTS.items()}
    concept = spark.read.csv("/Users/zolensky/Code/meds-biobank/data/PMBB-OMOP/concept.csv", header=True, inferSchema=True)
    concept = {str(row["concept_id"]): row["concept_name"] for row in concept.collect()}
    concept = concept | CONCEPTS_CUSTOM

    # plot patient events using help of concept table
    print(f"Plotting patient: {patient_id}, visit: {visit_id}")
    plot_events_emoji(meds_events_dict, concept, visit_id)