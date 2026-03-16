# A Modern Web Scraper for the RPI Student Information System (SIS)

This repository contains a Python web scraper for Rensselaer Polytechnic Institute's (RPI) new Student Information System (SIS), launched in September 2025.

The scraper gets data from various endpoints within the student registration system, which is accessible through [this link](https://sis9.rpi.edu/StudentRegistrationSsb/ssb/registration) without authentication.

Minimal setup is required to run the scraper; see [Running the SIS Scraper](#running-the-sis-scraper).

## Data Collected

The scraper fetches course data for any specified range of years, covering Spring, Summer, and Fall terms.

Summer 1998 is the earliest available academic term in SIS.

The scraper generates a JSON file for each processed term (see [Sample Output](#sample-output-json-format)).

Below is an abstract overview of the data that the scraper collects:

- Subject and course codes (e.g. CSCI" and "1100")
- Course sections
  - CRNs
  - Sequence numbers
- Class titles
- Descriptions
- Corequisites
- ~~Prerequisites (with AND, OR relationships)~~ (Currently WIP)
- Crosslists
- Attributes
- Restrictions
  - Restriction types (e.g. major, school, campus)
- Credit hours
  - Minimum and maximum hours
- Faculty information
  - Names
  - Email addresses
  - Sections taught
- Meeting information
  - Location
  - Times
  - Days held
- Seat and waitlist information
  - Capacity
  - Registered
  - Open

## Running the SIS Scraper

> Note: The `python3` and `pip3` commands seen in the steps below may not work in Windows systems. If using Windows, try `python` and `pip` instead.

### 1. Installing Python

**The scraper requires Python >= 3.13 to run.** If you don't have an appropriate version of Python already, [download and install one from the official website.](https://www.python.org/)

After Python has been installed, ensure both `python3` and `pip3` are recognized in the command line.

```bash
python3 --version
pip3 --version
```

If either command throws a "command not found" error, ensure that Python is in the system PATH variable and that the command line has been refreshed to reflect the updated variable.

### 1.5 (Optional) Setting Up a Virtual Environment

To avoid cluttering the global Python package environment on your computer, it's recommended to create a virtual environment in the project root and activate it in order to keep packages scoped to this project only.

```powershell
# Create a virtual environment in the current directory
python3 -m venv .venv
```

Activating the environment works differently between Windows and Unix, as shown below.

**Windows**

```powershell
# Enter/activate the environment
.venv\Scripts\activate
```

**Unix**

```bash
# Enter/activate the environment
source .venv/bin/activate
```

You can confirm the virtual environment is active if a (.venv) prefix appears in your command prompt.

```bash
# Note the (.venv)
(.venv) raymond@Macbook-Pro sis_scraper %
```

To exit/deactivate the virtual environment, simply run the deactivate command.

```bash
(.venv) raymond@Macbook-Pro sis_scraper % deactivate
raymond@Macbook-Pro sis_scraper %
```

After you've created and activated a virtual environment, continue with the following steps to install the required packages into the environment.

### 2. Installing Required Dependencies

From the project root, run the command below to install the required dependencies.

```powershell
# Install all packages listed in requirements.txt
pip3 install -r requirements.txt
```

### 3. Creating the .env File

Create a new file in the `src` directory named `.env`. An `example.env` file has been provided there for reference; you may simply copy-paste its contents to use the default values.

The file contains configuration variables for output directories, code map filenames, and so on. You may optionally edit them to your liking.

### 4. Running the Script

Navigate to the directory containing the source code if you haven't already.

```powershell
# Navigate to the src directory
cd src
```

Then, run `main.py` using one of the commands below.

```powershell
# Replace start_year and end_year with your desired time range
python3 main.py scrape start_year end_year
```

Once the scraper is running, logs will be printed to the console as well as to log files in the same directory as the source code.

The scraper's execution time varies depending on the number of terms being scraped, the amount of data available in each term, as well as external server or network factors that are not within the scraper's control. Overall, execution time scales linearly with the number of terms processed.

For reference, it typically takes my machine 20 minutes to scrape all terms between 1998 and 2026.

### 4.5 (Optional) Postprocessing and Database Pipelining

In addition to the SIS scraper, this repository includes scripts for postprocessing the scraper's JSON output and constructing a MySQL database from the postprocessed data.

These scripts format the scraper's output specifically for Project CARPI, but are documented here for anyone who might find the functionality useful.

**Postprocessing**

```powershell
# Postprocess all scraper JSON data
python main.py postprocess
```

This command processes all JSON data in the scraper output directory (configurable in `.env`) and saves the results to a separate directory, leaving the original data untouched.

- Reduce class attributes and restrictions to just their codes.
  - Attribute "Communication Intensive COMM" becomes "COMM"
  - Restriction "Architecture (ARCH)" becomes "ARCH"
- Reduce faculty data to just their RCSID and sections taught.
  - For faculty with an RPI email address, their RCSID is extracted from the address.
  - Otherwise, a fake RCSID is generated for that faculty, as an attempt at normalization.
  - All instances of faculty that share the same name and have no RPI email address share the same generated RCSID (i.e. treated as the same person).
- Reduce subject names to their code equivalent.
  - "Computer Science 1100" becomes "CSCI 1100"
- Extracts mappings for attributes, instructors, restrictions, and subjects, preserving them in separate JSON files alongside the processed data.
  - Attribute code-to-name map
  - Generated instructor RCSID-to-name map
  - Instructor RCSID-to-name map
  - Restriction code-to-name map
  - Subject code-to-name map

**Database Pipelining**

```powershell
# Commit postprocessed data to a relational database
python main.py commitdb
```

This command reads the postprocessed JSON data, constructs a MySQL database using the Project CARPI schema, and commits it directly to the database defined in your `.env` file.

Each time this command is run, any existing tables matching the schema will be dropped (along with their data) before the new data is committed.

## Sample Output JSON Format

The example below uses partially fabricated data to illustrate the structure. Note that this represents the raw scraped output before any postprocessing.

```json
{
  "CSCI": {
    "subjectDescription": "Computer Science",
    "courses": {
      "1100": [
        {
          "courseReferenceNumber": "75323",
          "sectionNumber": "01",
          "title": "COMPUTER SCIENCE I",
          "description": "An introduction to computer programming.",
          "attributes": [
            "Data Intensive I  DI1",
            "Introductory Level Course  FRSH"
          ],
          "restrictions": {
            "campus": [
              "Troy (T)"
            ],
            "not_campus": [
              "Hartford (H)"
            ],
            "classification": [
              "Freshman (FR)"
            ],
            "not_classification": [],
            "college": [
              "School of Science (S)"
            ],
            "not_college": [
              "Humanities, Arts & Soc Sci (H)"
            ],
            "degree": [],
            "not_degree": [
              "Doctor of Philosophy (PHD)"
            ],
            "department": [],
            "not_department": [],
            "level": [
              "Undergraduate (UG)"
            ],
            "not_level": [
              "Graduate (GR)"
            ],
            "major": [
              "Computer Science (CSCI)"
            ],
            "not_major": [
              "Architecture (ARCH)"
            ],
            "minor": [],
            "not_minor": [],
            "special_approval": [
              "Instructor's Signature"
            ]
          },
          "prerequisites": {},
          "corequisites": [
            {
              "subjectName": "Computer Science",
              "courseNumber": "1200",
              "title": "DATA STRUCTURES"
            }
          ],
          "crosslists": [
            {
              "courseReferenceNumber": "12345",
              "subjectName": "Computer Science",
              "courseNumber": "1150",
              "title": "COMPUTER SCIENCE II",
              "sectionNumber": "01"
            }
          ],
          "creditMin": 4,
          "creditMax": null,
          "seatsCapacity": 30,
          "seatsRegistered": 30,
          "seatsAvailable": 0,
          "waitlistCapacity": 0,
          "waitlistRegistered": 0,
          "waitlistAvailable": 0,
          "faculty": [
            {
              "displayName": "John Doe",
              "emailAddress": "doej@rpi.edu",
              "allMeetings": [
                2
              ],
              "primaryMeetings": [
                2
              ]
            },
            {
              "displayName": "Mary Jane",
              "emailAddress": "janem@rpi.edu",
              "allMeetings": [
                2
              ],
              "primaryMeetings": []
            }
          ],
          "meetingInfo": [
            {
              "id": 1,
              "beginTime": "1000",
              "endTime": "1150",
              "creditHours": 0.0,
              "campusCode": "T",
              "campusDescription": "Troy",
              "buildingCode": "DARRIN",
              "buildingDescription": "Darrin Communications Center",
              "category": "B",
              "room": "232",
              "startDate": "08/28/2025",
              "endDate": "12/12/2025",
              "days": [
                "T"
              ]
            },
            {
              "id": 2,
              "beginTime": "1200",
              "endTime": "1320",
              "creditHours": 4.0,
              "campusCode": "T",
              "campusDescription": "Troy",
              "buildingCode": "DARRIN",
              "buildingDescription": "Darrin Communications Center",
              "category": "L",
              "room": "308",
              "startDate": "08/28/2025",
              "endDate": "12/12/2025",
              "days": [
                "M",
                "R"
              ]
            }
          ]
        },
        ...
      ]
    }
  },
  ...
}
```
