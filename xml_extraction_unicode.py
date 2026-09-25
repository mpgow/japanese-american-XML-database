import sqlite3 # SQL database
import xml.etree.ElementTree as ET # python API for parsing XML trees
import os # used for walking through directory to parse files
import fugashi

tagger = fugashi.Tagger()

# Defines the relative starting location for directory search to be where the script file is located,
# so that script should still work regardless of working directory if executing in terminal.
# Otherwise, running in an IDE should be able to path resolve fine without this.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

class file:
    # TODO: decide what metadata we should collect from the files
    # Technically will likely never initialize an entry with these values, except manually
    def __init__(self, id=None, filePath=None, newspaper=None, date=None, pageNumber=None, pageConfidence=None, OCRSoftware=None, text=None, tokenizedText=None):
        self.id = id                         # from flushToDisk
        self.filePath = filePath             # from loop parse
        self.newspaper = newspaper           # from loop parse
        self.date = date                     # from extractFile
        self.pageNumber = pageNumber         # from extractFile
        self.pageConfidence = pageConfidence # from extractFile
        self.OCRSoftware = OCRSoftware       # from extractFile
        self.text = text                     # from extractFile
        self.tokenizedText = tokenizedText   # from extractFile, no redundant copy in pages

def createDatabase(dbname):
    # TODO: create database and call parsing function to create table
    connection = sqlite3.connect(f"{dbname}")
    cursor = connection.cursor()
    # filepath example: ...\UCB\nws_ShinSekai Asahi_The New World Sun\1940\05\05_01
    cursor.execute("""CREATE TABLE IF NOT EXISTS pages (
                   id INTEGER PRIMARY KEY,
                   filepath TEXT NOT NULL,
                   newspaper TEXT,
                   date TEXT,
                   pageNumber INTEGER,
                   pageConfidence REAL,
                   OCRSoftware TEXT,
                   text TEXT
                   )""")
    # choose text column for indexing since that's likely what we'll do our phrase searches on
    # specify content location to avoid duplicating all of the database text locally
    # content_rowid (currently assigned but ignored) by default uses implicit rowid that SQLite assigns each row,
    # in current state, pages and pages_fts aligned via flushToDisk
    # unicode splits on whitespace, fugashi splits logical phrases with whitespaces
    # join FTS5 table with original to link tokenizedText and other metadata
    # contentless table means tokenizedText thrown away after reverse-index generated
    # but can't use snippet() or highlight(), bc only position not text stored, join to recreate snippet
    cursor.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts
                   USING fts5(tokenizedText, content="", content_rowid="rowid", tokenize="unicode61")""")
    # EDIT r"UCB" VALUE TO CONFORM TO YOUR RELATIVE FOLDER LOCATION (note example above)
    rootLocation = os.path.join(SCRIPT_DIR, r"UCB") # smartly handles OS-dependent path creation
    cursor.execute("SELECT filepath FROM pages")
    alreadyIndexed = {row[0] for row in cursor.fetchall()} # good for re-running after a crash, skip already scanned files
    cursor.execute("SELECT COALESCE(MAX(id), 0) FROM pages") # use explicit ID we assigned
    nextID = cursor.fetchone()[0] + 1
    batchSize = 100 # For every 100 files, upload and commit for crash safety (and while fitting in memory)
    batchPages = []
    batchFTS = []
    for row in directoryParse(rootLocation, alreadyIndexed, nextID):
        # row = (filePath, newspaper, date, pageNumber, pageConfidence, OCRSoftware, text, tokenizedText)
        batchPages.append(row[:8]) #exclude tokenizedText
        batchFTS.append((row[0], row[8])) # just id and tokenizedText
        if (len(batchPages)) >= batchSize:
            flushToDisk(cursor, batchPages, batchFTS)
            batchPages.clear()
            batchFTS.clear()
            connection.commit()
    if (len(batchPages) > 0): # at end of directory tree, flush any partial final batches
        flushToDisk(cursor, batchPages, batchFTS)
        connection.commit()
    connection.close()

def flushToDisk(cursor, batchPages, batchFTS):
    cursor.executemany("INSERT INTO pages(id, filepath, newspaper, date, pageNumber, pageConfidence, OCRSoftware, text) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batchPages)
    cursor.executemany("INSERT INTO pages_fts(rowid, tokenizedText) VALUES (?, ?)", batchFTS) # pages_fts is basically an index on pages


def directoryParse(dirRootPath, alreadyIndexed, startingID):
    # TODO: loop through all newspapers, dates, and file pages to generate each full entry
    if (not os.path.isdir(dirRootPath)): 
        print(f"'{dirRootPath}' is not a valid directory.")
        return
    currentID = startingID
    for dirRoot, dirName, files in os.walk(dirRootPath):
        for fileName in files:
            if (fileName.lower().endswith(".xml")): # requires xml filetype, ignoring files with "xml" at end of name
                filePath = os.path.join(dirRoot, fileName)
                # checking if the filePath has already been inserted, if so then skip processing
                if (filePath in alreadyIndexed):
                    print(f"skip {filePath}")
                    continue
                # dirRoot (fuller) includes everything in path except the final filename
                # dirRootPath (shorter) includes path up to \UCB, since we joined last that in root_location
                relPath = os.path.relpath(dirRoot, dirRootPath)
                relParts = relPath.split(os.sep) # split path into array based on OS path separators
                currNewspaper = relParts[0] # first subfolder in \UCB is the newspaper titled folders themselves
                cF = file(filePath=filePath, newspaper=currNewspaper) # current file being extracted
                try:
                    extractFile(entry=cF, filePath=filePath)
                except Exception as e:
                    print(f"Failed on {filePath}: {e}")
                    continue
                # TODO: add validation
                yield (currentID, cF.filePath, cF.newspaper, cF.date, cF.pageNumber, cF.pageConfidence, cF.OCRSoftware, cF.text, cF.tokenizedText) # yield generator to insert file by file
                currentID += 1

# insted of default adding spaces between every OCR "word," use ascii + alphanumeric check 
# to ensure we only add spaces between latin-alphabetical words (English) and numbers
# japanese words remain unspaced within a sentence, as it is naturally written
def joinOCRChar(strings):
    words = []
    prevChar = ''
    for s in strings:
        if (s):
            currChar = s[0]
        else:
            currChar = ''
        prevEnglish = prevChar.isascii() or prevChar in "“”‘’" # directional quotes from auto formatting
        currEnglish = currChar.isascii() or currChar in "“”‘’"
        if (words and prevEnglish and currEnglish):
            if not ((currChar in ",.?!;:)}]”’") or (prevChar in "({[“‘")): # skip closing punctuation or after specific opening punctuation
                words.append(' ')
        words.append(s)
        if (s):
            prevChar = s[-1]
        else:
            prevChar = ''
    return ''.join(words)

# Inserts all the page's text and relevant metadata into the collection database 
def extractFile(entry, filePath):
    print(f"starting extraction of {filePath}")
    ns = "{http://www.loc.gov/standards/alto/ns-v3#}" # implicit namespace before all tags
    # Take in a filepath and reads in XML data
    tree = ET.parse(filePath) # can be adjacent filename or specific filepath
    root = tree.getroot() # <alto>
    # Gather embedded metadata
    fnText = root.find(f"{ns}Description/{ns}sourceImageInformation/{ns}fileName").text # redundancy
    date = fnText[6:10]+'-'+fnText[10:12]+'-'+fnText[12:14] # example: ./nws_19350805_0002.xml -> 1935-08-05
    OCRSoftwareRoot = root.find(f"{ns}Description/{ns}OCRProcessing/{ns}ocrProcessingStep/{ns}processingSoftware")
    OCRSoftware = ' '.join([OCRSoftwareRoot.find(f"{ns}softwareName").text, OCRSoftwareRoot.find(f"{ns}softwareVersion").text])
    pageNumber = root.find(f"{ns}Layout/{ns}Page").get("PHYSICAL_IMG_NR")
    pageConfidence = root.find(f"{ns}Layout/{ns}Page").get("PC")
    # Find all strings within the page
    textRoot = root.find(f"{ns}Layout/{ns}Page/{ns}PrintSpace") # steps into <alto>:[2]<Layout>[0]<Page>[4]<PrintSpace>
    strings = textRoot.iter(f"{ns}String") # ET iterator of all (nested) strings in textRoot
    # Concatenate strings into a single text
    text = joinOCRChar([s.get("CONTENT") for s in strings]) # space separate each OCR "word"
    tokenizedText = ' '.join([word.surface for word in tagger(text)])
    print (text)
    # TODO: Run a tokenizer to speed up keyword searches in queries
        # May want to address OCR corruptions first (e.g. there's no technique that re-merges text that has been
        # incorrectly split by an OCR error, so a 2 char word split with whitespace is never remerged with any logic)
    # Sets all metadata, raw text (for user readability),
    # (and nested tokenized words) to be inserted into collection database 
    entry.date = date
    entry.OCRSoftware = OCRSoftware
    entry.text = text
    entry.tokenizedText = tokenizedText
    entry.pageNumber = pageNumber
    entry.pageConfidence = pageConfidence

# For testing
if __name__ == "__main__":
    createDatabase("test_newspaper_explicitid_whitespacelogicwithpunctuation.db") # As I run this, I only have the tnw_ShinSekai_The New World & nws_ShinSekai Asahi_The New World Sun folders inside the relative directory \UCB
