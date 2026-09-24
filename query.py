import sqlite3
import os
import fugashi

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR,"test_newspaper_withexplicitid.db")

connection = sqlite3.connect(DB_PATH)
cursor = connection.cursor()
tagger = fugashi.Tagger()
# phrase = "明治"
phrase = input("what phrase or term are you searching for? ")
# TODO: Do bilingual phrase search, so inputs can be in romaji or kanji, with case-sensitivity choice
print(f"phrase received: {phrase}")
tokenizedPhrase = ' '.join(word.surface for word in tagger(phrase)) # ensure uses same format as when stored by tagger
ftsQuery = f'"{tokenizedPhrase}"' # double quotes to ensure strict matching
# pages row = (0:id, 1:filePath, 2:newspaper, 3:date, 4:pageNumber, 5:pageConfidence, 6:OCRSoftware, 7:text)
# fts row = (0:id, 1:tokenizedText)

cursor.execute("SELECT COUNT(*) FROM pages")
print(f"Total pages = {cursor.fetchone()[0]}")
cursor.execute("SELECT DISTINCT newspaper FROM pages")
print(f"Newspapers = {[row[0] for row in cursor.fetchall()]}")
cursor.execute("SELECT MIN(date), MAX(date) FROM pages")
row = cursor.fetchone()
print(f"Date range = {row[0]} to {row[1]}")
cursor.execute("SELECT filepath, date, pageConfidence, text FROM pages LIMIT 1")
# row = cursor.fetchone()
# print(f"\nSample Row:")
# print(f"FilePath = {row[0]}")
# print(f"Date = {row[1]}")
# print(f"Page Confidence = {row[2]}")
# print(f"Text = {row[3][:200]}")
cursor.execute("SELECT COUNT(*) FROM pages_fts WHERE tokenizedText MATCH ?", [ftsQuery]) # choose phrase here e.g. 二世
print(f"\nFTS page count search for {phrase}: {cursor.fetchone()[0]} results") # f's implicit rowid matched with p's explicit id
cursor.execute("""SELECT SUBSTR(date, 1, 4) as year, COUNT(*)
               FROM pages_fts f JOIN pages p ON f.rowid = p.id
               WHERE tokenizedText MATCH ?
               AND date >= '1800-01-01' AND date <= '2040-12-31'
               GROUP BY year
               ORDER BY year ASC""", [ftsQuery])
print(f"\nFTS page frequency search for \"{phrase}\" results")
results = cursor.fetchall()
if results:
    for row in results:
        year = row[0]
        count = row[1]
        print(f"Year {year}: {count} page appearances")
else:
    print("No results found")
print(f"\nFTS frequency search for \"{phrase}\" results")
cursor.execute("""SELECT p.text
               FROM pages p JOIN pages_fts f ON p.id = f.rowid
               WHERE f.tokenizedText MATCH ? 
               """, [ftsQuery]) # need custom logic for searching true appearances
trueTotal = 0
phraseTokens = tokenizedPhrase.split() # search for phrase as list of tokens, regardless of length
phraseLen = len(phraseTokens)
for row in cursor.fetchall(): # each "row" is actually a newspaper page that contains the tokenizedText
    pageTokens = [word.surface for word in tagger(row[0])]
    for i in range(len(pageTokens) - phraseLen + 1):
        if pageTokens[i:i + phraseLen] == phraseTokens:
            trueTotal += 1 # can now do true appearance counts regardless of phrase length
            # Search via fugashi's phrase format rather than p's OCR delimited words
    # trueTotal += row[0].split().count(tokenizedPhrase) #.count(tokenizedPhrase) assumes the search phrase is one token
    # if you want to search for a phrase LONGER than a token, you'll need to sequence match the tokens
    # for the page appearance counts, FTS5 should still work even with sequences of tokens
print(f"{trueTotal} total appearances")
# TODO: Refine search results
dateRange = input("Type in a date range in the form XXXX-XXXX: ")
startYear, endYear = dateRange[0:4], dateRange[5:] # TODO: needs more specificity for edge cases later
while len(dateRange) != 9 or not startYear.isdigit() or not endYear.isdigit():
    print("incorrect date format, try again")
    dateRange = input("Type in a date range in the form XXXX-XXXX: ")
startYear, endYear = int(dateRange[0:4]), int(dateRange[5:])
sourceRange = input("Select newspapers you'd like to include (WIP, input \"None\") ")
while sourceRange != "None":
    print("input must be None (WIP)")
    sourceRange = input("Select newspapers you'd like to include (WIP, input \"None\") ")

index = input("Type in the numerical index of the sentence you'd like to see the phrase used in: ")
while (not index.isdigit()):
    print("input must be an integer, try again")
    index = input("Type in the numerical index of the sentence you'd like to see the phrase used in: ")
index = int(index)
# TODO: Output the selected sentence with phrase -> note the index increments with every appearance, so multiple appearances in a page is possible
cursor.execute("""SELECT p.text, p.date
                  FROM pages p JOIN pages_fts f ON p.id = f.rowid
                  WHERE f.tokenizedText MATCH ?
                  """, [ftsQuery])
selectedTotal = 0
for SText, SDate in cursor.fetchall():
    # print(SText, SDate)
    print(f">>>{SDate} aka {int(SDate[0:4])} with selectedTotal: {selectedTotal}<<<")
    if selectedTotal == index:
        print(SText)
    pageTokens = [word.surface for word in tagger(row[0])]
    for i in range(len(pageTokens) - phraseLen + 1):
        if pageTokens[i:i + phraseLen] == phraseTokens and int(SDate[0:4]) >= startYear and int(SDate[0:4]) <= endYear:
            selectedTotal += 1
connection.close()