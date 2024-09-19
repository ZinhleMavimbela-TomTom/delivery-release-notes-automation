import os
import re
import csv
from bs4 import BeautifulSoup
import mysql.connector

# Environment Variables
REGION = os.getenv("REGIONS", "").strip()
VERSION = os.getenv("VERSION", "").strip()
REMOVE_KEYWORDS = os.getenv("REMOVE_WORD", "").strip()
CHANGE_FROM = os.getenv("CHANGE_PARAMETER1", "").strip()
CHANGE_TO = os.getenv("CHANGE_PARAMETER2", "").strip()

# Constants
CSV_FILE_PATH = 'Country-names.csv'
HTML_FILE_DIRECTORY = f'/share/nds-sources/products/commercial/{REGION}{VERSION}/documentation/mn/release_notes/release_notes/whats_new/'
HTML_FILE_PATTERN = f'highlights_and_improvements_mn_{REGION}_{VERSION}.html'

# Data Containers
country_data_list = []
unmatched_countries = []

class CountryInfo:
    """
    Represents the information of a country including its name, version, ISO code, and description.
    """
    def __init__(self, name, version, iso_code, description):
        self.name = name
        self.version = version
        self.iso_code = iso_code
        self.description = description

    def __str__(self):
        return f"Country Name: {self.name}, ISO Code: {self.iso_code}, Version: {self.version}\nDescription: {self.description}"

def insert_into_database(version, iso_code, description):
    """
    Inserts or updates the country information into the MySQL database.

    Args:
        version (str): Data source version.
        iso_code (str): Country ISO code.
        description (str): Highlights or description of the country.
    """
    # Retrieve database credentials from environment variables
    db_user = os.getenv('USER_NAME')
    db_password = os.getenv('MTC_AUTOBUILD_PASS')
    db_host = os.getenv('MTC_AUTOBUILD_HOST')
    db_name = os.getenv('DB_NAME')

    try:
        connection = mysql.connector.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            database=db_name
        )
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO release_notes (data_source_version, country, highlights)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE highlights = VALUES(highlights)
        """
        cursor.execute(insert_query, (version, iso_code, description))
        connection.commit()
    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def get_iso_code(country_name):
    """
    Matches the country name with its ISO code from the CSV file.

    Args:
        country_name (str): The name of the country.

    Returns:
        str or None: The ISO code if a match is found; otherwise, None.
    """
    try:
        with open(CSV_FILE_PATH, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header row
            for row in reader:
                iso_code, csv_country_name = row[0].strip(), row[1].strip()
                if csv_country_name.lower() == country_name.lower():
                    return iso_code
                elif re.search(re.escape(csv_country_name), country_name, re.IGNORECASE):
                    return iso_code
    except FileNotFoundError:
        print(f"CSV file not found at path: {CSV_FILE_PATH}")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
    return None

def parse_html_file():
    """
    Parses the HTML file to extract country names and their descriptions.
    """
    html_file_path = os.path.join(HTML_FILE_DIRECTORY, HTML_FILE_PATTERN)

    try:
        with open(html_file_path, 'r', encoding='utf-8') as file:
            soup = BeautifulSoup(file, 'html.parser')

            # Extract data source version from the HTML title
            title_tag = soup.find('title')
            if title_tag and len(title_tag.text.split()) >= 3:
                data_source_version = title_tag.text.split()[2]
            else:
                data_source_version = "Unknown"
                print("Warning: Unable to extract data source version from title.")

            # Extract country names from h2 tags with class 'CountryName'
            country_name_tags = soup.find_all("h2", class_="CountryName")
            country_names = [sanitize_text(tag.text) for tag in country_name_tags]

            # Extract descriptions from ul tags with class 'CountryRemark'
            description_tags = soup.find_all("ul", class_="CountryRemark")
            descriptions = [extract_descriptions(ul_tag) for ul_tag in description_tags]

            # Remove the first entry if it is 'General'
            if country_names and country_names[0].lower() == "general":
                country_names.pop(0)
                descriptions.pop(0)

            # Match ISO codes for each country
            iso_codes = []
            for name in country_names:
                iso = get_iso_code(name)
                iso_codes.append(iso)
                if iso is None:
                    unmatched_countries.append(name)

            # Ensure all lists have the same length
            if len(country_names) == len(iso_codes) == len(descriptions):
                for name, iso, desc in zip(country_names, iso_codes, descriptions):
                    country_data = CountryInfo(
                        name=name,
                        version=data_source_version,
                        iso_code=iso,
                        description="\n".join(desc)
                    )
                    country_data_list.append(country_data)
            else:
                print("Error: Mismatch in the number of countries, ISO codes, or descriptions.")
                print(f"Countries: {len(country_names)}, ISO Codes: {len(iso_codes)}, Descriptions: {len(descriptions)}")
    except FileNotFoundError:
        print(f"HTML file not found at path: {html_file_path}")
    except Exception as e:
        print(f"Error parsing HTML file: {e}")

def sanitize_text(text):
    """
    Cleans up the input text by removing unwanted characters and spaces.

    Args:
        text (str): The text to sanitize.

    Returns:
        str: The sanitized text.
    """
    text = re.sub(r"[\n\t]+", "", text)
    text = re.sub(' +', " ", text)
    return text.strip()

def extract_descriptions(ul_tag):
    """
    Extracts and cleans descriptions from a given ul tag.

    Args:
        ul_tag (bs4.element.Tag): The ul tag containing description list items.

    Returns:
        list: A list of cleaned description strings.
    """
    descriptions = []
    list_items = ul_tag.find_all("li")
    for li in list_items:
        desc = sanitize_text(li.get_text())

        # Remove unwanted keywords
        if REMOVE_KEYWORDS:
            for keyword in REMOVE_KEYWORDS.split(","):
                desc = re.sub(re.escape(keyword), "", desc, flags=re.IGNORECASE)

        # Replace specified parameters
        if CHANGE_FROM and CHANGE_TO:
            from_terms = CHANGE_FROM.split(",")
            to_terms = CHANGE_TO.split(",")
            replacements = zip(from_terms, to_terms)
            for from_term, to_term in replacements:
                desc = re.sub(re.escape(from_term), to_term, desc, flags=re.IGNORECASE)

        descriptions.append(desc)
    return descriptions

def display_all_countries():
    """
    Prints all the country data for verification.
    """
    separator = "_" * 200
    print(separator)
    for country in country_data_list:
        print(separator)
        print(country)
    print(separator)

def upload_data_to_db():
    """
    Uploads all the country data to the MySQL database.
    """
    for country in country_data_list:
        insert_into_database(
            version=country.version,
            iso_code=country.iso_code,
            description=country.description
        )

def main():
    """
    Main function to orchestrate the parsing and uploading process.
    """
    parse_html_file()
    print(f"Total Country Count: {len(country_data_list)}")

    if not unmatched_countries:
        print("Uploading data to the database...")
        upload_data_to_db()
    else:
        print("ERROR: The following countries have no matching ISO codes in the CSV file. Please update the CSV and retry:")
        for country in unmatched_countries:
            print(f"- {country}")

    display_all_countries()

if __name__ == "__main__":
    main()