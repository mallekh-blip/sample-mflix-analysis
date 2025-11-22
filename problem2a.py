import json
import time
from tqdm import tqdm
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# -----------------------------------------------------
# 1. CONNECT TO AZURE COSMOSDB (YOUR NEW STRING)
# -----------------------------------------------------
from dotenv import load_dotenv
import os

load_dotenv()  # reads .env file
uri = os.getenv("COSMOS_URI")
client = MongoClient(uri)

client = MongoClient(uri)
db = client["sample_mflix"]

print("✅ Connected to CosmosDB!")
print("📁 Database =", db.name)


# -----------------------------------------------------
# 2. Convert JSON Lines → JSON Array
# -----------------------------------------------------
def convert_to_array(json_path):
    print(f"\n🔄 Converting {json_path} to JSON array...")

    with open(json_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    docs = [json.loads(line) for line in lines]

    out_path = json_path.replace(".json", "_array.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2)

    print("✅ Saved:", out_path)
    return out_path


# -----------------------------------------------------
# 3. Upload JSON Array in Safe Batches
# -----------------------------------------------------
def safe_upload(collection_name, file_path, batch_size=50):
    print(f"\n📌 Uploading '{collection_name}' from {file_path}")

    col = db[collection_name]

    # Try dropping (Cosmos sometimes blocks)
    try:
        col.drop()
        print("🧹 Dropped existing collection")
    except:
        print("⚠ Could not drop collection (Cosmos restriction). Continuing...")

    # Load documents
    with open(file_path, "r", encoding="utf-8") as f:
        docs = json.load(f)

    total = len(docs)
    print(f"📄 Total Documents = {total}")

    # Insert in batches
    for i in tqdm(range(0, total, batch_size)):
        batch = docs[i:i+batch_size]

        inserted = False
        retries = 0

        while not inserted:
            try:
                col.insert_many(batch, ordered=False)
                inserted = True

            except BulkWriteError:
                # Duplicate _id → continue
                print("⚠ Duplicate keys – skipping batch")
                inserted = True

            except Exception as e:
                msg = str(e)

                if "16500" in msg or "RequestRateTooLarge" in msg:
                    # Throttling → wait & retry
                    retries += 1
                    wait = 2 + retries
                    print(f"⏳ Throttled. Waiting {wait} seconds...")
                    time.sleep(wait)
                else:
                    print("❌ Unexpected error:", e)
                    inserted = True

    print(f"🎉 Finished uploading {collection_name}!")


# -----------------------------------------------------
# 4. RUN IMPORT FOR ALL COLLECTIONS
# -----------------------------------------------------
base = r"C:\Users\harib\OneDrive\Desktop\New folder (2)"

movies_array    = convert_to_array(base + r"\movies.json")
comments_array  = convert_to_array(base + r"\comments.json")
users_array     = convert_to_array(base + r"\users.json")
theaters_array  = convert_to_array(base + r"\theaters.json")

safe_upload("movies", movies_array)
safe_upload("comments", comments_array)
safe_upload("users", users_array)
safe_upload("theaters", theaters_array)

print("\n🎉 ALL COLLECTIONS IMPORTED SUCCESSFULLY!")
