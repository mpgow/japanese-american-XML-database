import sqlite3
import os
# import fugashi # moved to db creation
# import pykakasi # moved to db creation
# import re # for regex (not used yet)
from xml_extraction_unicode import joinOCRChar, tokenizeTriplet

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR,"test_newspaper_vers1.db")

connection = sqlite3.connect(DB_PATH)
cursor = connection.cursor()
# tagger = fugashi.Tagger()
# kks = pykakasi.kakasi()

# if tokens contain quotation marks, double it to make it SQL safe for querying
def ftsPhrase(tokens):
    return '"' + ' '.join(token.replace('"', '""') for token in tokens) + '"'

# returns the start indices where phrase is found in tokens
# only checks for entire phrase once first token of phrase matches current checked index
def phraseScanner(tokens, phrase):
    start = phrase[0] # first phrase token, use to qualify when to actually create a scanning slice
    found = [] # all indices that are the start of a fully matching phrase found in tokens
    for i in range(len(tokens)):
        if tokens[i] == start and tokens[i:i + len(phrase)] == phrase:
            found.append(i)
    return found

def lowercaseEach(tokens):
    return [token.lower() for token in tokens]

# phrase = "明治" # example single token kanji phrase
phrase = input("what phrase or term are you searching for? ")
# TODO: Do bilingual phrase search, so inputs can be in romaji or kanji, with case-sensitivity choice
# inputs can be kanji, kana, romaji, or English
# searching romaji or English just searches the input as typed without conversions
surfacePhrase, kanaPhrase, romaPhrase = tokenizeTriplet(phrase)
if not surfacePhrase:
    print(f"{phrase} has no searchable tokens")
    connection.close()
    exit()
phraseLen = len(surfacePhrase)
surfaceQuery = ftsPhrase(surfacePhrase)
romaQuery = ftsPhrase(romaPhrase)

# Need to take phrase, run it through tokenized Roma first to get superset's indices
# tokenized Roma -> total hits from superset of romanized text
# tokenizedText at the SAME index -> map of original text char : hit count
# tokenizedText is also used to still do total occurrence count of EXACT phrase if desired
print(f"phrase received: {phrase}")
print(f"phrase tokens = {surfacePhrase} : romanized reading = {romaPhrase}")
# pages row = (0:id, 1:filePath, 2:newspaper, 3:date, 4:pageNumber, 5:pageConfidence, 6:OCRSoftware, 7:text, 8:tokenizedText, 9:tokenizedKana, 10:tokenizedRoma)
# fts row = (0:id, 1:tokenizedText, 2:tokenizedRoma)

# General database overview for sense of scale
cursor.execute("SELECT COUNT(*) FROM pages")
print(f"Total pages = {cursor.fetchone()[0]}")
cursor.execute("SELECT DISTINCT newspaper FROM pages")
print(f"Newspapers = {[row[0] for row in cursor.fetchall()]}")
cursor.execute("SELECT MIN(date), MAX(date) FROM pages")
row = cursor.fetchone()
print(f"Date range = {row[0]} to {row[1]}")

# search romanized text for pages that contain the phrase, and then parse qualified pages to find exact indices and their writing to print out
cursor.execute("SELECT COUNT(*) FROM pages_fts WHERE tokenizedRoma MATCH ?", [romaQuery]) # choose phrase here e.g. 二世 : nisei
print(f"\nFTS page count search for romanized text \"{' '.join(romaPhrase)}\": {cursor.fetchone()[0]} results") # f's implicit rowid matched with p's explicit id
cursor.execute("""SELECT SUBSTR(date, 1, 4) as year, COUNT(*)
               FROM pages_fts f JOIN pages p ON f.rowid = p.id
               WHERE f.tokenizedRoma MATCH ?
               AND date >= '1800-01-01' AND date <= '2040-12-31'
               GROUP BY year
               ORDER BY year ASC""", [romaQuery])
print(f"\nFTS page frequency search for \"{phrase}\" results:")
results = cursor.fetchall()
if results:
    for row in results:
        year = row[0]
        count = row[1]
        print(f"Year {year}: {count} page appearances")
else:
    print("No results found")
    connection.close()
    exit() # TODO: Make phrase inputs part of a function to loop

# Go through the FTS qualified pages to count true occurrences
# match indices using the romanized tokens, and then read paired surface tokens at those indices
# this allows for broadband search and specific written form counts, without fugashi or pykakasi runs

print(f"\nFTS frequency search for \"{phrase}\" results")
cursor.execute("""SELECT p.tokenizedText, p.tokenizedRoma
               FROM pages p JOIN pages_fts f ON p.id = f.rowid
               WHERE f.tokenizedRoma MATCH ? 
               """, [romaQuery]) # need custom logic for searching true appearances
counts = {}
scanPhrase = lowercaseEach(romaPhrase)
for surfaceText, romaText in cursor: # load rows of pages one at a time
    surfaceTokens = surfaceText.split()
    romaTokens = romaText.split()
    scanTokens = lowercaseEach(romaTokens)
    for i in phraseScanner(scanTokens, scanPhrase):
        counts[' '.join(surfaceTokens[i:i + phraseLen])] = counts.get(' '.join(surfaceTokens[i:i + phraseLen]), 0) + 1
print(f"{sum(counts.values())} total appearances of shared case insensitive romanized text")
for pKey, pValue in counts.items():
    print(f"{pKey}: {pValue}")
print(f"{counts.get(' '.join(surfacePhrase), 0)} total appearances matched your exact input (case and transliteration sensitive)")

# TODO: Refine search results

while True:
    dateRange = input("Type in a date range in the form XXXX-XXXX: ")
    startYear, endYear = dateRange[0:4], dateRange[5:] # TODO: needs more specificity for edge cases later
    if len(dateRange) == 9 and startYear.isdigit() and endYear.isdigit():
        break
    print("incorrect date format, try again")
startYear, endYear = int(dateRange[0:4]), int(dateRange[5:])

sourceRange = input("Select newspapers you'd like to include (WIP, input \"None\") ")
while sourceRange != "None":
    print("input must be None (WIP)")
    sourceRange = input("Select newspapers you'd like to include (WIP, input \"None\") ")

exact = input("Transliteration Modes: { B = broad, any written form | E = exact, match spelling exactly} ")
while exact not in ("B", "E"):
    print("input must be B or E, try again")
    exact = input("Transliteration Modes: { B = broad, any written form | E = exact, match spelling exactly} ")
exact = (exact == "E")

index = input("Type in the numerical index of the sentence you'd like to see the phrase used in: ")
while (not index.isdigit()):
    print("input must be an integer, try again")
    index = input("Type in the numerical index of the sentence you'd like to see the phrase used in: ")
index = int(index)

sensitive = input("Would you like to match case-sensitively? { Y | N } ")
while (sensitive != "Y" and sensitive != "N"):
    print("input must be a single character, try again")
    sensitive = input("Would you like to match case-sensitively? { Y | N } ")
sensitive = (sensitive == "Y")

# TODO: Deprecate the original search method in favor of dual search version

# Find the exact sentence at the selected index
# Broad "B" mode searches roma column (the superset); exact "E" mode searches the surface column (matching raw)

matchColumn = "tokenizedText" if exact else "tokenizedRoma"
matchQuery = surfaceQuery if exact else romaQuery
matchPhrase = surfacePhrase if exact else romaPhrase
if not sensitive:
    matchPhrase = lowercaseEach(matchPhrase)

# Ensure index ordering with ORDER BY clause, and narrow search with year filter
cursor.execute(f"""SELECT p.tokenizedText, p.tokenizedRoma, p.date
                   FROM pages p JOIN pages_fts f ON p.id = f.rowid
                   WHERE f.{matchColumn} MATCH ?
                   AND CAST(SUBSTR(p.date, 1, 4) AS INTEGER) BETWEEN ? AND ?
                   ORDER BY p.date, p.id""", [matchQuery, startYear, endYear])
selectedTotal = 0
found = False
for surfaceText, romaText, SDate in cursor:
    surfaceTokens = surfaceText.split() # the original text that will be displayed
    matchTokens = surfaceTokens if exact else romaText.split() # the tokens our phrase will match check with
    if not sensitive:
        matchTokens = lowercaseEach(matchTokens) # only English changes, doesn't affect Japanese characters
    for i in phraseScanner(matchTokens, matchPhrase):
        if selectedTotal == index:
            sentence = []
            # lI scans left until reaches beginning of page or the end of the last sentence
            # rI scans right from the end of the found phrase until reaches the end of  page or the start of the next sentence 
            lI, rI = i - 1, i + phraseLen 
            while lI >= 0 and surfaceTokens[lI] not in ".。?!！": # fugashi tokenizes punctuation by itself
                # TODO: check if any character in the token is an ending punctuation
                sentence.append(surfaceTokens[lI])
                lI -= 1
            sentence = sentence[::-1] # reverse sentence because we scanned from right to left
            sentence.extend(surfaceTokens[i:i + phraseLen]) # add each token of the query phrase
            while rI < len(surfaceTokens):
                sentence.append(surfaceTokens[rI])
                if surfaceTokens[rI] in ".。?!！": # Need to specify more punctuation
                     # get all consecutive ending punctuation before breaking out of the sentence
                    if rI + 1 >= len(surfaceTokens) or surfaceTokens[rI + 1] not in ".。?!！":
                        break
                rI += 1
            print(f"[matched as: {' '.join(surfaceTokens[i:i + phraseLen])}] {SDate}")
            print(joinOCRChar(sentence))
            found = True
            break
        selectedTotal += 1
        print(f">>>{SDate} aka {int(SDate[0:4])} with selected total: {selectedTotal}<<<")
        # Might want to print confidence of that specific page as a sanity point
        # Also, want to reduce counting in terminal prints due to line scroll limits
    if found:
        break
if not found:
    print(f"only {selectedTotal} matches in that range, so index {index} is out of range")
connection.close()