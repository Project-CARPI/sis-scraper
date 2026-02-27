import requests
from bs4 import BeautifulSoup
import time


def main():
    print("Starting Scraper...")
    URL = "https://catalog.rpi.edu/content.php?catoid=30&navoid=864"
    storage_p = []
    storage_ul = []

    try:
        pageToScrape = requests.get(URL)
        if pageToScrape.status_code == 200:
            soup = BeautifulSoup(pageToScrape.text, "html.parser")
            portfolios = soup.find(
                "td", attrs={"class": "block_content", "colspan": "2"}
            )

            # 1. Get Degree Categories (e.g., Bachelor, Doctoral)
            p_names = portfolios.findAll("p", attrs={"style": "padding-left: 30px"})
            for name in p_names:
                strong_tag = name.find("strong")
                if strong_tag:
                    storage_p.append(strong_tag.get_text(strip=True))

            # 2. Get Degree Links and Names
            degree_links = portfolios.findAll("ul", attrs={"class": "program-list"})
            for degree_type in degree_links:
                current_degrees = []
                list_degrees = degree_type.findAll(
                    "li", attrs={"style": "list-style-type: none"}
                )
                for degree in list_degrees:
                    link_tag = degree.find("a")
                    if link_tag:
                        href = "https://catalog.rpi.edu/" + link_tag.get("href")
                        name = link_tag.get_text(strip=True)
                        current_degrees.append([name, href])
                storage_ul.append(current_degrees)

            # 3. Visit Individual Links (Example: Doctoral Degrees)
            for index, category in enumerate(storage_p):
                if category == "Doctoral":
                    print(f"\n--- Scraping {category} Degrees ---")
                    for degree_info in storage_ul[index]:
                        degree_name = degree_info[0]
                        degree_url = degree_info[1]

                        print(f"Accessing: {degree_name}")
                        scrape_degree_details(degree_url)

                        # Be nice to the server!
                        time.sleep(1)

    except Exception as e:
        print(f"An error occurred: {e}")


def scrape_degree_details(url):
    """Parses individual degree pages for requirements."""
    response = requests.get(url)
    if response.status_code != 200:
        return

    soup = BeautifulSoup(response.text, "html.parser")

    # RPI Catalog often puts requirements in 100% width tables or specific 'acalog-core' divs
    # This logic looks for headers (h2/h3) and the lists following them
    cores = soup.find_all("div", class_="acalog-core")

    for core in cores:
        # Extract the requirement header (e.g., 'Required Courses')
        header = core.find(["h2", "h3", "h4"])
        if header:
            print(f"  Section: {header.get_text(strip=True)}")

            # Find all course links/names in this section
            courses = core.find_all("li", class_="acalog-course")
            for course in courses:
                print(f"    - {course.get_text(strip=True)}")


if __name__ == "__main__":
    main()
