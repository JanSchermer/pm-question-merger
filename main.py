# Question merger by Jan Schermer
import hashlib
import traceback

import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials

# Output target:
# {
#   answers: ["A", "B", "C", "D"],
#   difficulty: 0,
#   question: "QUESTION2,
#   rightAnswer: "A",
#   category: "CATEGORY",
#   hash: "IM_A_SHA256_HASH",
# }

FIREBASE_CREDENTIALS_LOCATION = "credentials.json"
FIREBASE_COLLECTION_PATH = "questions"

QUESTION_CSV_LOCATION = "questions.csv"
QUESTION_CSV_ENCODING = "cp1252"
QUESTION_CSV_COLUMN_SEPERATOR = ";"

TABLE_HEADER_QUESTION_KEYWORD = "-Fragen"
TABLE_HEADER_ROW = 1
TABLE_HEADER_HEIGHT = 2

TABLE_COLUMN_OFFSET_DIFFICULTY = 0
TABLE_COLUMN_OFFSET_QUESTION = 1
TABLE_COLUMN_OFFSET_ANSWERS = [4, 5, 6, 7]
TABLE_COLUMN_OFFSET_RIGHT_ANSWER = 8

DIFFICULTY_MAP = {
    "EINFACH": 0,
    "MITTEL": 1,
    "SCHWER": 2
}

QUESTION_STRING_REPLACE_MAP = {
    '""': "'",
    '"': ""
}

firebase_app = None
firestore_instance = None
firestore_collection = None

def parse_entry_row(row, general_offset, category):
    difficulty = DIFFICULTY_MAP.get(row[TABLE_COLUMN_OFFSET_DIFFICULTY + general_offset].upper())
    if difficulty is None:
        return None
    answers = []
    for offset in TABLE_COLUMN_OFFSET_ANSWERS:
        answers.append(row[offset + general_offset])
    question = row[TABLE_COLUMN_OFFSET_QUESTION + general_offset]
    for to_replace in QUESTION_STRING_REPLACE_MAP:
        question = question.replace(to_replace, QUESTION_STRING_REPLACE_MAP[to_replace])
    right_answer = row[TABLE_COLUMN_OFFSET_RIGHT_ANSWER + general_offset]
    hash_sum = str(
        hashlib.sha256(
            "$"
            .join([str(answers), question, right_answer, str(difficulty)])
            .encode("utf-8")
        ).hexdigest())
    entry = {
        "answers": answers,
        "question": question,
        "rightAnswer": right_answer,
        "difficulty": difficulty,
        "category": category,
        "hash": hash_sum,
    }
    return entry


def parse_header_row(row):
    category_offsets = {}
    for i in range(len(row)):
        field = row[i]
        if TABLE_HEADER_QUESTION_KEYWORD not in field:
            continue
        field = field.replace(TABLE_HEADER_QUESTION_KEYWORD, "")
        category_offsets[field] = i
    return category_offsets


def parse_rows(rows):
    entries = []
    header_row = rows[TABLE_HEADER_ROW]
    category_offsets = parse_header_row(header_row)
    for category in category_offsets.keys():
        for i in range(TABLE_HEADER_ROW + TABLE_HEADER_HEIGHT, len(rows)):
            entry = parse_entry_row(rows[i], category_offsets[category], category)
            if not entry:
                break
            entries.append(entry)
    return entries


def read_rows_from_file():
    with open(QUESTION_CSV_LOCATION, "r", encoding=QUESTION_CSV_ENCODING) as f:
        rows = f.readlines()
    rows = [row.split(QUESTION_CSV_COLUMN_SEPERATOR) for row in rows]
    return rows


def print_progress(iteration, total, suffix, fill='█', printEnd = "\r"):
    percent = "{0:.1f}".format(100 * (iteration / float(total)))
    filledLength = int(100 * iteration // total)
    bar = fill * filledLength + '-' * (100 - filledLength)
    print(f'\rProgress |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


def init_firebase():
    global firebase_app, firestore_instance, firestore_collection
    cred = credentials.Certificate(FIREBASE_CREDENTIALS_LOCATION)
    firebase_app = firebase_admin.initialize_app(cred)
    firestore_instance = firebase_admin.firestore.client(app=firebase_app)
    firestore_collection = firestore_instance.collection(FIREBASE_COLLECTION_PATH)



rows = None
entries = None

try:
    init_firebase()
except:
    print(traceback.format_exc())
    print("\nCould not authenticate to FireBase using \"" + FIREBASE_CREDENTIALS_LOCATION + "\"")
    print("https://firebase.google.com/support/guides/service-accounts")
    exit(1)

print("Firebase connection Established.")

try:
    rows = read_rows_from_file()
except:
    print(traceback.format_exc())
    print("\nCould not read question data from \"" + QUESTION_CSV_LOCATION + "\"")
    print("Please export the question document from Excel as CSV and put it in this folder.")
    exit(1)

print("Loaded questions table into memory.")

try:
    entries = parse_rows(rows)
except:
    print(traceback.format_exc())
    print("\nThere was an error parsing the questions. Might the file be corrupt?")
    exit(1)

print("Parsed all " + str(len(entries)) + " questions.")

existing = firestore_collection.get()
firebase_count = len(existing)

print("Found " + str(firebase_count) + " questions already in firebase.")

print("\nMerging update into Firestore...")

available_ids = [str(i) for i in range(len(entries))]

already_uploaded = []
for entry in existing:
    entry_dict = entry.to_dict()
    hash = entry_dict["hash"]
    matches = [entry for entry in entries if entry["hash"] == hash]
    if len(matches) > 0:
        available_ids.remove(entry.id)
        already_uploaded.append(matches[0]["hash"])

skipped_count = len(already_uploaded)


print("Firestore already contains " + str(skipped_count) + " matching entries. Skipping.")

for entry in entries:
    if entry["hash"] in already_uploaded:
        continue
    print_progress(len(already_uploaded) - skipped_count + 1, len(entries) - skipped_count, "Updating entries")
    id = available_ids.pop()
    firestore_collection.document(id).set(entry)
    already_uploaded.append(entry["hash"])

# Delete documents out of id range
redundant_documents = [entry for entry in existing if entry.to_dict()["hash"] not in already_uploaded]
print("\nDeleting " + str(max(firebase_count - len(already_uploaded), 0)) + " redundant documents...")
while len(redundant_documents) > 0:
    id = redundant_documents.pop().id
    firestore_collection.document(str(id)).delete()

print("\n\nSuccessfully merged \"" + QUESTION_CSV_LOCATION + "\" into remote collection: \"" + FIREBASE_COLLECTION_PATH + "\"")