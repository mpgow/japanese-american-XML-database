# japanese-american-XML-database
Plan: convert XML files from Japanese American newspapers up to 1942 into an indexed database, readily available for linguistic analysis.

## Usage
install, find, and uninstalling fugashi package for tokenization by running this in your terminal:

```bash
pip install fugashi unidic-lite
pip show fugashi
pip uninstall fugashi unidic-lite
```

run xml-extraction-unicode.py in order to first generate a reusable indexed database file.

if database creation fails or crashes at any point, feel free to re-run the extraction python file. It will continue where it left off.

If any files are to be added, deleted, or updated, it's advisable to create a new database file from scratch via the extractor.


## Notes:
XML files appear to be organized by the ALTO-XML schema:
https://en.wikipedia.org/wiki/Analyzed_Layout_and_Text_Object

gitignore -> prevents specified files from being tracked

