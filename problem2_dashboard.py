import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
from wordcloud import WordCloud
from dotenv import load_dotenv
import os

# ---------------------------------------------------
# 1️⃣ Loading environment variables
# ---------------------------------------------------
load_dotenv()

COSMOS_URI = os.getenv("COSMOS_URI")

if not COSMOS_URI:
    st.error(" COSMOS_URI missing. Add it inside your .env file.")
    st.stop()

# ---------------------------------------------------
# 2️⃣ Connecting to Cosmos DB
# ---------------------------------------------------
try:
    client = MongoClient(COSMOS_URI)
    db = client["sample_mflix"]
except Exception as e:
    st.error(f" Connection failed: {e}")
    st.stop()

st.title("Azure CosmosDB Movie Analytics Dashboard")

st.markdown("### Interactive dashboard connected to your cloud-hosted MongoDB/CosmosDB dataset.")

# ---------------------------------------------------
# 3️⃣ Loading Movies Collection
# ---------------------------------------------------
movies = list(db.movies.find(
    {},
    {
        "title": 1,
        "genres": 1,
        "year": 1,
        "plot": 1,
        "cast": 1,
        "directors": 1,
        "imdb": 1,      # loading IMDB as dictionary
        "_id": 0
    }
))

movies_df = pd.DataFrame(movies)

# ---------------------------------------------------
# 4️⃣ Fixing the Year Field
# ---------------------------------------------------
movies_df["year"] = pd.to_numeric(movies_df["year"], errors="coerce")

# ---------------------------------------------------
# 5️⃣ Fixing nested imdb.rating field safely
# ---------------------------------------------------
def extract_rating(imdb_block):
    if isinstance(imdb_block, dict):
        return imdb_block.get("rating")
    return None

movies_df["rating"] = movies_df["imdb"].apply(extract_rating)
movies_df["rating"] = pd.to_numeric(movies_df["rating"], errors="coerce")

# ---------------------------------------------------
# SIDEBAR CONTROLS
# ---------------------------------------------------
st.sidebar.header(" Filters")

# Genre filter
all_genres = sorted({g for row in movies_df["genres"].dropna() for g in row})
selected_genre = st.sidebar.selectbox("Select Genre", ["All Genres"] + all_genres)

# Year filter — clean NaN years
valid_years = movies_df["year"].dropna()
min_year, max_year = int(valid_years.min()), int(valid_years.max())

year_range = st.sidebar.slider("Select Year Range", min_year, max_year, (1980, max_year))

rating_filter = st.sidebar.slider("Minimum Rating", 0.0, 10.0, 5.0)

# ---------------------------------------------------
# APPLING FILTERS
# ---------------------------------------------------
filtered = movies_df.copy()

if selected_genre != "All Genres":
    filtered = filtered[
        filtered["genres"].apply(
            lambda x: selected_genre in x if isinstance(x, list) else False
        )
    ]

filtered = filtered[
    (filtered["year"] >= year_range[0]) &
    (filtered["year"] <= year_range[1]) &
    (filtered["rating"] >= rating_filter)
]

st.subheader("🎥 Filtered Movies")
st.write(filtered[["title", "year", "genres", "rating"]].head(20))

# ---------------------------------------------------
# 6️⃣ Rating Trend Over Years
# ---------------------------------------------------
trend_data = filtered.dropna(subset=["rating", "year"])

if len(trend_data) > 0:
    trend = (
        trend_data.groupby("year")["rating"]
        .mean()
        .reset_index()
        .sort_values("year")
    )

    st.subheader("📈 Rating Trend Over Years")
    st.line_chart(trend, x="year", y="rating")
else:
    st.info("No movies available for selected filters.")

# ---------------------------------------------------
# 7️⃣ Top Actors / Directors
# ---------------------------------------------------
st.subheader(" Top Cast Members (Most Appearances)")

cast_counts = {}
for cast in movies_df["cast"].dropna():
    for person in cast:
        cast_counts[person] = cast_counts.get(person, 0) + 1

cast_df = (
    pd.DataFrame(cast_counts.items(), columns=["Actor", "Appearances"])
    .sort_values(by="Appearances", ascending=False)
    .head(20)
)

st.bar_chart(cast_df.set_index("Actor"))

# ---------------------------------------------------
# 8️⃣ Word Cloud of Movie Plots
# ---------------------------------------------------
st.subheader("☁️ Movie Plot Word Cloud")

all_plots = " ".join([str(p) for p in movies_df["plot"].dropna()])

wc = WordCloud(width=900, height=400, background_color="white").generate(all_plots)

plt.imshow(wc, interpolation="bilinear")
plt.axis("off")
st.pyplot(plt)

# ---------------------------------------------------
# 9️⃣ Comments Summary
# ---------------------------------------------------
st.subheader("💬 Most Active Commenters")

comments = list(db.comments.find({}, {"email": 1, "_id": 0}))
comments_df = pd.DataFrame(comments)

top_users = comments_df["email"].value_counts().head(10)
st.bar_chart(top_users)

# ---------------------------------------------------
# END
# ---------------------------------------------------
st.success("🎉 Dashboard Loaded Successfully!")
