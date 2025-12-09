import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import re
import numpy as np

# --- CONFIGURATION ---
DB_CONFIG = {
    "dbname": "movies",
    "user": "postgres",
    "password": "pamungkas",
    "host": "localhost",
    "port": "5432"
}

# Postgres Limits
MAX_BIGINT = 9223372036854775807
MAX_INT = 2147483647

# --- HELPER FUNCTIONS ---

def clean_year_complex(text):
    """
    Parses complex year formats based on specific business rules.
    Returns a String (TEXT) not an Integer.
    """
    if pd.isna(text): return "0"
    text = str(text).strip()
    
    # 1. Remove Roman Numeral prefixes like (I), (II) from the start
    # Regex: Start of string, parenthesis, Roman numerals, closing paren
    text = re.sub(r'^\([IVX]+\)\s*', '', text)
    
    # 2. Case: (2021– ) -> 2021 - Present
    # Check for Year + En-dash/Hyphen + Space + closing paren
    match_present = re.search(r'\((\d{4})[–-]\s*\)', text)
    if match_present:
        return f"{match_present.group(1)} - Present"
        
    # 3. Case: (2010–2022) -> 2010–2022
    # Check for Year + Dash + Year
    match_range = re.search(r'\((\d{4})[–-](\d{4})\)', text)
    if match_range:
        return f"{match_range.group(1)}–{match_range.group(2)}"
        
    # 4. General Case: Just extract the first 4-digit year found
    # Handles: (2016 TV Movie), (2018 Video Game), (1993)
    match_simple = re.search(r'(\d{4})', text)
    if match_simple:
        return match_simple.group(1)
        
    return "0"

def clean_money_column(val):
    if pd.isna(val) or val == "": return 0.0
    val = str(val).replace('$', '').replace('M', '').replace(',', '').strip()
    try:
        if not val: return 0.0
        return float(val) * 1_000_000
    except ValueError:
        return 0.0

def parse_stars_field(text):
    if pd.isna(text): return [], []
    text = str(text).replace('\n', '').strip()
    directors, stars = [], []
    parts = text.split('|')
    for part in parts:
        if 'Director' in part:
            clean_part = re.sub(r'Directors?:', '', part).strip()
            directors = [x.strip() for x in clean_part.split(',') if x.strip()]
        elif 'Star' in part:
            clean_part = re.sub(r'Stars?:', '', part).strip()
            stars = [x.strip() for x in clean_part.split(',') if x.strip()]
    return directors, stars

def safe_cast_int(val, max_limit):
    try:
        if pd.isna(val) or str(val).strip() == "": return 0
        num = float(val)
        if num > max_limit or num < -max_limit:
            return 0 
        return int(num)
    except:
        return 0

# --- ETL PROCESS ---
def process_and_load(csv_file):
    print("Step 1: Reading CSV...")
    df = pd.read_csv(csv_file)
    
    print("Step 2: Cleaning Data & Formatting Years...")
    
    # 1. Clean Title
    df['Title'] = df['MOVIES'].astype(str).str.strip()
    
    # 2. Clean Description (Strip whitespace/newlines)
    df['Description_Clean'] = df['ONE-LINE'].astype(str).str.strip()
    
    # 3. Clean Year (NEW LOGIC)
    # We apply the custom function element-wise
    df['Year_Clean'] = df['YEAR'].apply(clean_year_complex)

    # 4. Clean Genre
    df['Genre_Clean'] = df['GENRE'].astype(str).str.replace('\n', '').str.strip()
    
    # 5. Clean Rating
    df['Rating_Clean'] = pd.to_numeric(df['RATING'], errors='coerce').fillna(0.0)
    
    # 6. Clean Votes
    df['Votes_Temp'] = (df['VOTES'].astype(str)
                        .str.replace(',', '', regex=False)
                        .str.extract(r'(\d+)')[0]) 
    df['Votes_Clean'] = df['Votes_Temp'].apply(lambda x: safe_cast_int(x, MAX_BIGINT))

    # 7. Clean Runtime
    df['RunTime_Clean'] = pd.to_numeric(df['RunTime'], errors='coerce')
    df['RunTime_Clean'] = df['RunTime_Clean'].apply(lambda x: safe_cast_int(x, MAX_INT))

    # 8. Clean Gross
    df['Gross_Clean'] = df['Gross'].apply(clean_money_column)

    # 9. Parse Lists
    print("Step 3: Parsing Directors and Stars...")
    parsed_col = df['STARS'].apply(parse_stars_field) 
    df['Directors_List'] = parsed_col.apply(lambda x: x[0])
    df['Stars_List'] = parsed_col.apply(lambda x: x[1])

    # --- DATABASE LOADING ---
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Step 4: Resetting Schema (Year -> TEXT)...")
        tables = ["Movie_Genres", "Movie_Stars", "Movie_Directors", "Movies", "Genres", "Persons"]
        for t in tables: cursor.execute(f"DROP TABLE IF EXISTS {t} CASCADE;")
            
        cursor.execute("CREATE TABLE Persons (PersonID SERIAL PRIMARY KEY, Name TEXT NOT NULL UNIQUE);")
        cursor.execute("CREATE TABLE Genres (GenreID SERIAL PRIMARY KEY, Name TEXT NOT NULL UNIQUE);")
        
        # UPDATED TABLE DEFINITION: Year is now TEXT
        cursor.execute("""
            CREATE TABLE Movies (
                MovieID SERIAL PRIMARY KEY, 
                Title TEXT NOT NULL, 
                Year TEXT, 
                Rating DECIMAL(3, 1), 
                Description TEXT, 
                Votes BIGINT, 
                RunTime INTEGER, 
                Gross NUMERIC
            );
        """)
        cursor.execute("CREATE TABLE Movie_Directors (MovieID INT, PersonID INT, PRIMARY KEY(MovieID, PersonID));")
        cursor.execute("CREATE TABLE Movie_Stars (MovieID INT, PersonID INT, PRIMARY KEY(MovieID, PersonID));")
        cursor.execute("CREATE TABLE Movie_Genres (MovieID INT, GenreID INT, PRIMARY KEY(MovieID, GenreID));")
        conn.commit()

        print("Step 5: Bulk Inserting Dimensions...")
        all_directors = set(x for l in df['Directors_List'] for x in l)
        all_stars = set(x for l in df['Stars_List'] for x in l)
        all_persons = list(all_directors.union(all_stars))
        
        all_genres = set()
        for g_str in df['Genre_Clean']:
            if g_str and g_str != 'nan':
                all_genres.update([x.strip() for x in g_str.split(',')])
        all_genres = list(all_genres)
        
        execute_values(cursor, "INSERT INTO Persons (Name) VALUES %s ON CONFLICT DO NOTHING", [(p,) for p in all_persons])
        execute_values(cursor, "INSERT INTO Genres (Name) VALUES %s ON CONFLICT DO NOTHING", [(g,) for g in all_genres])
        
        cursor.execute("SELECT Name, PersonID FROM Persons")
        person_map = {row[0]: row[1] for row in cursor.fetchall()}
        cursor.execute("SELECT Name, GenreID FROM Genres")
        genre_map = {row[0]: row[1] for row in cursor.fetchall()}
        
        print("Step 6: Inserting Movies & Relations...")
        
        movie_directors_data = []
        movie_stars_data = []
        movie_genres_data = []
        
        for index, row in df.iterrows():
            try:
                # Insert Movie with new Year Format
                cursor.execute(
                    "INSERT INTO Movies (Title, Year, Rating, Description, Votes, RunTime, Gross) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING MovieID",
                    (row['Title'], row['Year_Clean'], row['Rating_Clean'], row['Description_Clean'], row['Votes_Clean'], row['RunTime_Clean'], row['Gross_Clean'])
                )
                movie_id = cursor.fetchone()[0]
                
                # Relations
                for d in row['Directors_List']:
                    if d in person_map: movie_directors_data.append((movie_id, person_map[d]))
                for s in row['Stars_List']:
                    if s in person_map: movie_stars_data.append((movie_id, person_map[s]))
                if row['Genre_Clean'] and row['Genre_Clean'] != 'nan':
                    for g in row['Genre_Clean'].split(','):
                        g = g.strip()
                        if g in genre_map: movie_genres_data.append((movie_id, genre_map[g]))
                        
            except Exception as e:
                print(f"Error on Row {index} ({row['Title']}): {e}")
                conn.rollback() 
                continue 

        print("Step 7: Saving Junctions...")
        execute_values(cursor, "INSERT INTO Movie_Directors (MovieID, PersonID) VALUES %s ON CONFLICT DO NOTHING", movie_directors_data)
        execute_values(cursor, "INSERT INTO Movie_Stars (MovieID, PersonID) VALUES %s ON CONFLICT DO NOTHING", movie_stars_data)
        execute_values(cursor, "INSERT INTO Movie_Genres (MovieID, GenreID) VALUES %s ON CONFLICT DO NOTHING", movie_genres_data)

        conn.commit()
        print("ETL Success! Year column successfully transformed.")

    except Exception as e:
        print(f"Fatal Error: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    process_and_load('movies.csv')