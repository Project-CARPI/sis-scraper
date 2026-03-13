from bs4 import BeautifulSoup
import requests


def main():
    print("Starting Scrape...")
    URL = "https://catalog.rpi.edu/content.php?catoid=30&navoid=864"

    headers = {"User-Agent": "Mozilla/5.0"}
    pageToScrape = requests.get(URL, headers=headers)

    if pageToScrape.status_code != 200:
        print(f"Failed to retrieve page: {pageToScrape.status_code}")
        return

    soup = BeautifulSoup(pageToScrape.text, "html.parser")
    portfolios = soup.find("td", attrs={"class": "block_content", "colspan": "2"})

    # 1. Map Degree Types to their respective lists
    # We find all degree headers (e.g., 'Master's', 'Doctorate')
    p_names = portfolios.findAll("p", attrs={"style": "padding-left: 30px"})
    # We find all lists of programs following those headers
    program_lists = portfolios.findAll("ul", attrs={"class": "program-list"})

    master_degrees = []

    # 2. Iterate through headers to find "Master’s"
    # Zip them together so p_names[0] matches program_lists[0]
    for header, program_list in zip(p_names, program_lists):
        degree_type = header.get_text(strip=True)

        if "Master" in degree_type:
            print(f"Found Category: {degree_type}")

            # 3. Extract all links within this specific Master's section
            list_items = program_list.findAll(
                "li", attrs={"style": "list-style-type: none"}
            )
            for li in list_items:
                link_tag = li.find("a")
                if link_tag:
                    name = link_tag.get_text(strip=True)
                    href = "https://catalog.rpi.edu/" + link_tag.get("href")
                    master_degrees.append({"name": name, "link": href})

    # Output results
    print(f"\nFound {len(master_degrees)} Master's Degrees:")
    for degree in master_degrees:
        print(f"- {degree['name']} ({degree['link']})")


if __name__ == "__main__":
    main()
