import requests
from bs4 import BeautifulSoup
import time
import csv
from tqdm import tqdm  # COOL: Visual progress bars

# Professional touch: Identify as a browser so the server doesn't block you
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def main():
    print("🚀 Initializing RPI Degree Scraper...")
    URL = "https://catalog.rpi.edu/content.php?catoid=30&navoid=864"

    storage_p = []
    storage_ul = []
    final_data = []  # To store all rows for CSV

    try:
        pageToScrape = requests.get(URL, headers=HEADERS)
        if pageToScrape.status_code == 200:
            soup = BeautifulSoup(pageToScrape.text, "html.parser")
            portfolios = soup.find(
                "td", attrs={"class": "block_content", "colspan": "2"}
            )

            # 1. Get Degree Categories
            p_names = portfolios.findAll("p", attrs={"style": "padding-left: 30px"})
            for name in p_names:
                strong_tag = name.find("strong")
                if strong_tag:
                    storage_p.append(strong_tag.get_text(strip=True))

            # 2. Get Degree Links
            degree_links = portfolios.findAll("ul", attrs={"class": "program-list"})
            for degree_type in degree_links:
                current_degrees = []
                list_degrees = degree_type.findAll("li")  # Simplified selector
                for degree in list_degrees:
                    link_tag = degree.find("a")
                    if link_tag:
                        href = "https://catalog.rpi.edu/" + link_tag.get("href")
                        name = link_tag.get_text(strip=True)
                        current_degrees.append([name, href])
                storage_ul.append(current_degrees)

            # 3. Visit Links with a Progress Bar
            for index, category in enumerate(storage_p):
                if category == "Doctoral":
                    print(f"\n📂 Processing Category: {category}")

                    # tqdm creates a cool animated progress bar in your terminal
                    for degree_info in tqdm(
                        storage_ul[index], desc="Scraping Degrees", unit="pg"
                    ):
                        degree_name = degree_info[0]
                        degree_url = degree_info[1]

                        details = scrape_degree_details(degree_url)

                        for section, course in details:
                            final_data.append([category, degree_name, section, course])

                        time.sleep(0.5)  # Polite delay

            # 4. COOL: Export to CSV
            save_to_csv(final_data)

    except Exception as e:
        print(f"❌ An error occurred: {e}")


def scrape_degree_details(url):
    """Parses individual degree pages for requirements."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        cores = soup.find_all("div", class_="acalog-core")
        degree_data = []

        for core in cores:
            header = core.find(["h2", "h3", "h4"])
            section_name = (
                header.get_text(strip=True) if header else "General Requirements"
            )

            courses = core.find_all("li", class_="acalog-course")
            for course in courses:
                course_name = course.get_text(strip=True)
                degree_data.append([section_name, course_name])

        return degree_data
    except:
        return []


def save_to_csv(data):
    """Saves the scraped data to a structured CSV file."""
    filename = "rpi_doctoral_degrees.csv"
    headers = ["Category", "Degree Name", "Section", "Course"]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

    print(f"\n✨ Success! Data saved to {filename}")


if __name__ == "__main__":
    main()
