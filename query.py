import sqlite3
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
DB_PATH = os.path.join(SCRIPT_DIR,"test_tnw.db")

connection = sqlite3.connect(DB_PATH)
cursor = connection.cursor()

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
cursor.execute("SELECT COUNT(*) FROM pages_fts WHERE text MATCH '新世界'") # choose phrase here e.g. 二世
print(f"\nFTS search for 新世界: {cursor.fetchone()[0]} results")
connection.close()