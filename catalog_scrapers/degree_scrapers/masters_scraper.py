from bs4 import BeautifulSoup
import requests
import csv
import json


BASE_URL = "https://catalog.rpi.edu/"
URL = BASE_URL + "content.php?catoid=30&navoid=864"


def fetch_page(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code != 200:
            print(f"Failed to retrieve page: {response.status_code}")
            return None

        return response.text

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None


def parse_masters(html, keyword=None):
    soup = BeautifulSoup(html, "html.parser")

    portfolios = soup.find("td", attrs={"class": "block_content", "colspan": "2"})
    p_names = portfolios.findAll("p", attrs={"style": "padding-left: 30px"})
    program_lists = portfolios.findAll("ul", attrs={"class": "program-list"})

    master_degrees = []

    for header, program_list in zip(p_names, program_lists):
        degree_type = header.get_text(strip=True)

        if "Master" in degree_type:
            print(f"Found Category: {degree_type}")

            list_items = program_list.findAll(
                "li", attrs={"style": "list-style-type: none"}
            )

            for li in list_items:
                link_tag = li.find("a")
                if link_tag:
                    name = link_tag.get_text(strip=True)
                    href = BASE_URL + link_tag.get("href")

                    # Optional keyword filter
                    if keyword and keyword.lower() not in name.lower():
                        continue

                    master_degrees.append({"name": name, "link": href})

    # Remove duplicates
    unique_degrees = {d["name"]: d for d in master_degrees}
    return list(unique_degrees.values())


def save_to_csv(data, filename="masters_degrees.csv"):
    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Degree Name", "Link"])

        for degree in data:
            writer.writerow([degree["name"], degree["link"]])


def save_to_json(data, filename="masters_degrees.json"):
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)


def main():
    print("Starting Scrape...\n")

    html = fetch_page(URL)
    if not html:
        return

    # Optional: filter by keyword (e.g., "Engineering")
    keyword = None  # change to something like "Data" if needed

    degrees = parse_masters(html, keyword)

    print(f"\nTotal Master's Degrees Found: {len(degrees)}")

    save_to_csv(degrees)
    save_to_json(degrees)

    print("\nData saved to:")
    print("- masters_degrees.csv")
    print("- masters_degrees.json")


if __name__ == "__main__":
    main()
