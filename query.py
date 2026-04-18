import sqlite3
import os
import fugashi

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
DB_PATH = os.path.join(SCRIPT_DIR,"test_newspaper.db")

connection = sqlite3.connect(DB_PATH)
cursor = connection.cursor()
tagger = fugashi.Tagger()
phrase = "世界 "
tokenizedPhrase = ' '.join(word.surface for word in tagger(phrase)) # ensure uses same format as when stored by tagger
ftsQuery = f'"{tokenizedPhrase}"' # double quotes to ensure strict matching
# pages row = (0:filePath, 1:newspaper, 2:date, 3:pageNumber, 4:pageConfidence, 5:OCRSoftware, 6:text)
# fts row = (0:tokenizedText)

cursor.execute("SELECT COUNT(*) FROM pages")
print(f"Total pages = {cursor.fetchone()[0]}")
cursor.execute("SELECT DISTINCT newspaper FROM pages")
print(f"Newspapers = {[row[0] for row in cursor.fetchall()]}")
cursor.execute("SELECT MIN(date), MAX(date) FROM pages")
row = cursor.fetchone()
print(f"Date range = {row[0]} to {row[1]}")
cursor.execute("SELECT filepath, date, pageConfidence, text FROM pages LIMIT 1")
row = cursor.fetchone()
print(f"\nSample Row:")
print(f"FilePath = {row[0]}")
print(f"Date = {row[1]}")
print(f"Page Confidence = {row[2]}")
print(f"Text = {row[3][:200]}")
cursor.execute("SELECT COUNT(*) FROM pages_fts WHERE tokenizedText MATCH ?", [ftsQuery]) # choose phrase here e.g. 二世
print(f"\nFTS count search for {phrase}: {cursor.fetchone()[0]} results")
cursor.execute("""SELECT SUBSTR(date, 1, 4) as year, COUNT(*) 
               FROM pages_fts f JOIN pages p ON f.rowid = p.rowid
               WHERE tokenizedText MATCH ? 
               AND date >= '1800-01-01' AND date <= '2040-12-31'
               GROUP BY year
               ORDER BY year ASC""", [ftsQuery])
print(f"\nFTS frequency search for {phrase} results")
results = cursor.fetchall()
if results:
    for row in results:
        year = row[0]
        count = row[1]
        print(f"Year {year}: {count} appearances")
else:
    print("No results found")
connection.close()