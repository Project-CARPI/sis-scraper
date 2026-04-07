from bs4 import BeautifulSoup
import requests
import csv
import os


def main():
    print("Hello")
    URL = "https://catalog.rpi.edu/content.php?catoid=30&navoid=864"
    storage_p = []
    storage_ul = []
    pageToScrape = requests.get(URL)
    while True:
        if pageToScrape.status_code == 200:
            soup = BeautifulSoup(pageToScrape.text, "html.parser")
            portfolios = soup.find(
                "td", attrs={"class": "block_content", "colspan": "2"}
            )

            # get all the degree names
            p_names = portfolios.findAll("p", attrs={"style": "padding-left: 30px"})
            for name in p_names:
                strong = name.find("strong").get_text()
                storage_p.append(strong)

            # get all the degree links and names of degrees
            degree_links = portfolios.findAll("ul", attrs={"class": "program-list"})
            for degree_type in degree_links:
                current_degrees = []
                list_degrees = degree_type.findAll(
                    "li", attrs={"style": "list-style-type: none"}
                )
                for degree in list_degrees:
                    href = "https://catalog.rpi.edu/" + degree.find("a").get("href")
                    name = degree.find("a").get_text()
                    name_link_pair = [name, href]
                    current_degrees.append(name_link_pair)
                storage_ul.append(current_degrees)
            # - All the degrees in each type
            # print(storage_ul)
            # - Type of degrees
            # print(storage_p)
            # time to visit individual links and find the credit requirements!
            # use "acalog index % 3" to get the year, fall semester, and spring semester.

            # more issues:
            # 1. Not all the pages are in the same format. You will indeed have a total of 8 acalog_core's and 4 headers per undergraduate degree (check COMD for the error)
            # 2. Try using a counter instead of intentionally shortening the acalog_core list. The code will need a lot of work.
            classes_and_requirements = {}
            for index_of_degree in range(len(storage_p)):
                # print(storage_ul[index_of_degree])
                # bachelors
                if storage_p[index_of_degree] == "Baccalaureate":
                    for degree in storage_ul[index_of_degree]:
                        print(degree[0])
                        # ERROR LOG:
                        #   Biology: ???
                        #   Engineering Core Curriculum: fall_classes = all_leftpads[i]... --> index out of range
                        #   Information Technology and Web Science: Completely different scraping format.
                        #   Music: ???
                        #   Physician-Scientist: spring_classes = all_leftpads[i]... -> NoneType object
                        if (
                            degree[0] == "Nuclear Engineering Curriculum (B.S)"
                            or degree[0] == "Chemical Engineering, B.S."
                            or degree[0]
                            == "Information Technology and Web Science B.S."
                            or degree[0] == "Architecture, B.Arch."
                        ):
                            continue
                        classes_and_requirements[degree[0]] = {
                            "First Year": {"Fall": [], "Spring": []},
                            "Second Year": {"Fall": [], "Spring": []},
                            "Third Year": {"Arch": [], "Fall": [], "Spring": []},
                            "Fourth Year": {"Fall": [], "Spring": []},
                        }

                        # ONE ISSUE FOR POP: Check if something exists in list. If not, dont pop

                        link = degree[1]
                        requirements = requests.get(link)
                        # print(classes_and_requirements)
                        if requirements.status_code == 200:
                            soup = BeautifulSoup(requirements.text, "html.parser")
                            portfolios = soup.find(
                                "td", attrs={"colspan": "4", "class": "width"}
                            )
                            all_info = portfolios.find(
                                "div", attrs={"class": "custom_leftpad_20"}
                            )
                            # get all acalog-cores (first thru. fourth years)
                            all_leftpads = all_info.findAll(
                                "div",
                                attrs={"class": "custom_leftpad_20"},
                                recursive=False,
                            )[0:4]

                            if degree[0] == "Engineering Core Curriculum":
                                for i in range(2):
                                    fall_sem = []
                                    spring_sem = []
                                    fall_classes = []
                                    spring_classes = []

                                    fall_val = (
                                        all_leftpads[i]
                                        .findAll("div", attrs={"class": "acalog-core"})[
                                            0
                                        ]
                                        .findAll("ul")
                                    )
                                    spring_val = (
                                        all_leftpads[i]
                                        .findAll("div", attrs={"class": "acalog-core"})[
                                            1
                                        ]
                                        .findAll("ul")
                                    )

                                    for a in fall_val:
                                        fall_classes += a.findAll("li")
                                    for b in spring_val:
                                        spring_classes += b.findAll("li")
                                    if len(fall_val) > 1:
                                        lol = []
                                        for j in range(0, len(fall_val)):
                                            lol += fall_val[j].findAll("li")

                                        fall_val = lol

                                    if len(spring_val) > 1:
                                        lol = []
                                        for j in range(0, len(spring_val)):
                                            lol += spring_val[j].findAll("li")

                                        spring_val = lol

                                    processed_sem = []

                                    def process_semester_classes(class_list):
                                        index = 0

                                        while index < len(class_list):
                                            text = class_list[index].get_text()

                                            try:
                                                # Standard class processing
                                                colon_idx = text.index(":")
                                                # Logic: class name is before colon (minus 13 chars), credits is after
                                                class_item = text[
                                                    : colon_idx - 13
                                                ].replace("….", "... -")
                                                credits = text[
                                                    colon_idx + 2 : colon_idx + 3
                                                ]
                                                processed_sem.append(
                                                    f"{class_item}:{credits}"
                                                )
                                                index += 1

                                            except ValueError:
                                                # Handle "or" logic for alternative classes
                                                if (
                                                    text.strip().lower() == "or"
                                                    and index > 0
                                                    and index + 1 < len(class_list)
                                                ):
                                                    or_group = []

                                                    # Pop the previous class to group it with the "or" option
                                                    if processed_sem:
                                                        processed_sem.pop()

                                                    # Helper to parse specific indices for the "or" logic
                                                    for i in [index - 1, index + 1]:
                                                        choice_text = class_list[
                                                            i
                                                        ].get_text()
                                                        c_idx = choice_text.index(":")

                                                        # Note: Using your specific slicing logic for first vs second choice
                                                        offset = (
                                                            13 if i == index - 1 else 0
                                                        )
                                                        name = choice_text[
                                                            : c_idx - offset
                                                        ]
                                                        creds = choice_text[
                                                            c_idx + 2 : c_idx + 3
                                                        ]
                                                        or_group.append(
                                                            f"{name}:{creds}"
                                                        )

                                                    processed_sem.append(or_group)
                                                    index += 2  # Skip the "or" and the second choice
                                                else:
                                                    index += 1

                                        return processed_sem

                                    # Main Execution
                                    if i == 0:
                                        year_data = classes_and_requirements[degree[0]][
                                            "First Year"
                                        ]

                                        year_data["Fall"] = process_semester_classes(
                                            fall_classes
                                        )
                                        year_data["Spring"] = process_semester_classes(
                                            spring_classes
                                        )
                                    elif i == 1:
                                        # print("Sophomore year loading")
                                        index = 0
                                        while index < len(fall_classes):
                                            class_and_credits = fall_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = (
                                                    class_and_credits[0 : test - 13]
                                                    .replace("….", "... -")
                                                    .replace("….", "... -")
                                                )
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                fall_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(fall_sem) > 0:
                                                        fall_sem.pop(len(fall_sem) - 1)
                                                    first_choice = fall_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = fall_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    fall_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1
                                        # do the same thing with spring classes
                                        index = 0
                                        while index < len(spring_classes):
                                            class_and_credits = spring_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                spring_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(spring_sem) > 0:
                                                        spring_sem.pop(
                                                            len(spring_sem) - 1
                                                        )
                                                    first_choice = spring_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = spring_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    spring_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1
                                        # add all the courses into the requirements
                                        classes_and_requirements[degree[0]][
                                            "Second Year"
                                        ]["Fall"] = fall_sem
                                        classes_and_requirements[degree[0]][
                                            "Second Year"
                                        ]["Spring"] = spring_sem
                            elif degree[0] == "Physician-Scientist":
                                for i in range(3):
                                    arch_sem = []
                                    fall_sem = []
                                    spring_sem = []
                                    arch_classes = []
                                    # fall_classes = all_leftpads[i].findAll('div', attrs={'class':'acalog-core'})[0].find('ul').findAll('li')
                                    # spring_classes = all_leftpads[i].findAll('div', attrs={'class':'acalog-core'})[1].find('ul').findAll('li')

                                    fall_val = (
                                        all_leftpads[i]
                                        .findAll("div", attrs={"class": "acalog-core"})[
                                            0
                                        ]
                                        .findAll("ul")
                                    )
                                    spring_val = (
                                        all_leftpads[i]
                                        .findAll("div", attrs={"class": "acalog-core"})[
                                            1
                                        ]
                                        .findAll("ul")
                                    )

                                    for a in fall_val:
                                        fall_classes += a.findAll("li")
                                    for b in spring_val:
                                        spring_classes += b.findAll("li")

                                    if len(fall_val) > 1:
                                        lol = []
                                        for j in range(0, len(fall_val)):
                                            lol += fall_val[j].findAll("li")

                                        fall_val = lol

                                    if len(spring_val) > 1:
                                        lol = []
                                        for j in range(0, len(spring_val)):
                                            lol += spring_val[j].findAll("li")

                                        spring_val = lol

                                    if i == 2:
                                        arch_classes = (
                                            all_leftpads[i]
                                            .findAll(
                                                "div", attrs={"class": "acalog-core"}
                                            )[0]
                                            .find("ul")
                                            .findAll("li")
                                        )
                                        fall_classes = (
                                            all_leftpads[i]
                                            .findAll(
                                                "div", attrs={"class": "acalog-core"}
                                            )[1]
                                            .find("ul")
                                            .findAll("li")
                                        )
                                        # spring_classes = all_leftpads[i].findAll('div', attrs={'class':'acalog-core'})[2].find('ul').findAll('li')

                                    if i == 0:
                                        index = 0
                                        while index < len(fall_classes):
                                            class_and_credits = fall_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                fall_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(fall_sem) > 0:
                                                        fall_sem.pop(len(fall_sem) - 1)
                                                    first_choice = fall_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = fall_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    fall_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1

                                        # do the same thing with spring classes
                                        index = 0
                                        while index < len(spring_classes):
                                            class_and_credits = spring_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                spring_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(spring_sem) > 0:
                                                        spring_sem.pop(
                                                            len(spring_sem) - 1
                                                        )
                                                    first_choice = spring_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = spring_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    spring_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1
                                        # add all the courses into the requirements
                                        classes_and_requirements[degree[0]][
                                            "First Year"
                                        ]["Fall"] = fall_sem
                                        classes_and_requirements[degree[0]][
                                            "First Year"
                                        ]["Spring"] = spring_sem
                                        # print(classes_and_requirements)
                                    elif i == 1:
                                        # print("Sophomore year loading")
                                        index = 0
                                        while index < len(fall_classes):
                                            class_and_credits = fall_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = (
                                                    class_and_credits[0 : test - 13]
                                                    .replace("….", "... -")
                                                    .replace("….", "... -")
                                                )
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                fall_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(fall_sem) > 0:
                                                        fall_sem.pop(len(fall_sem) - 1)
                                                    first_choice = fall_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = fall_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    fall_sem.append(or_classes)
                                                    index += 2
                                                    # testign
                                                else:
                                                    index += 1
                                        # do the same thing with spring classes
                                        index = 0
                                        while index < len(spring_classes):
                                            class_and_credits = spring_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                spring_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(spring_sem) > 0:
                                                        spring_sem.pop(
                                                            len(spring_sem) - 1
                                                        )
                                                    first_choice = spring_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = spring_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    spring_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1
                                        # add all the courses into the requirements
                                        classes_and_requirements[degree[0]][
                                            "Second Year"
                                        ]["Fall"] = fall_sem
                                        classes_and_requirements[degree[0]][
                                            "Second Year"
                                        ]["Spring"] = spring_sem
                                        # print(classes_and_requirements)
                                        # sys.exit(1)

                                    # TO DO
                                    # Arch semesters are different. Approach must be different.
                                    elif i == 2:
                                        # print(arch_classes)
                                        # print(arch_sem)
                                        # print(fall_classes)
                                        # print(spring_classes)
                                        # return
                                        index = 0
                                        while index < len(arch_classes):
                                            class_and_credits = arch_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                arch_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(arch_sem) > 0:
                                                        arch_sem.pop(len(arch_sem) - 1)

                                                    first_choice = arch_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = arch_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    arch_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1
                                        index = 0
                                        while index < len(fall_classes):
                                            class_and_credits = fall_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                fall_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(fall_sem) > 0:
                                                        fall_sem.pop(len(fall_sem) - 1)
                                                    first_choice = fall_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = fall_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    fall_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1
                                        classes_and_requirements[degree[0]][
                                            "Third Year"
                                        ]["Arch"] = arch_sem
                                        classes_and_requirements[degree[0]][
                                            "Third Year"
                                        ]["Fall"] = fall_sem
                                        classes_and_requirements[degree[0]][
                                            "Third Year"
                                        ]["Spring"] = [
                                            "ILEA 4400 - Independent Learning Experience:0"
                                        ]
                            else:
                                for i in range(4):
                                    arch_sem = []
                                    fall_sem = []
                                    spring_sem = []
                                    arch_classes = []
                                    fall_classes = []
                                    spring_classes = []
                                    fall_val = (
                                        all_leftpads[i]
                                        .findAll("div", attrs={"class": "acalog-core"})[
                                            0
                                        ]
                                        .findAll("ul")
                                    )
                                    spring_val = (
                                        all_leftpads[i]
                                        .findAll("div", attrs={"class": "acalog-core"})[
                                            1
                                        ]
                                        .findAll("ul")
                                    )

                                    for a in fall_val:
                                        fall_classes += a.findAll("li")
                                    for b in spring_val:
                                        spring_classes += b.findAll("li")

                                    # fall_val = all_leftpads[i].findAll('div', attrs={'class':'acalog-core'})[0].findAll('ul')
                                    # spring_val = all_leftpads[i].findAll('div', attrs={'class':'acalog-core'})[1].findAll('ul')
                                    if len(fall_val) > 1:
                                        lol = []
                                        for j in range(0, len(fall_val)):
                                            lol += fall_val[j].findAll("li")

                                        fall_val = lol

                                    if len(spring_val) > 1:
                                        lol = []
                                        for j in range(0, len(spring_val)):
                                            lol += spring_val[j].findAll("li")

                                        spring_val = lol

                                    if i == 2:
                                        arch_classes = (
                                            all_leftpads[i]
                                            .findAll(
                                                "div", attrs={"class": "acalog-core"}
                                            )[0]
                                            .find("ul")
                                            .findAll("li")
                                        )
                                        fall_classes = (
                                            all_leftpads[i]
                                            .findAll(
                                                "div", attrs={"class": "acalog-core"}
                                            )[1]
                                            .find("ul")
                                            .findAll("li")
                                        )
                                        # spring_classes = all_leftpads[i].findAll('div', attrs={'class':'acalog-core'})[2].find('ul').findAll('li')

                                    if i == 0:
                                        index = 0
                                        while index < len(fall_classes):
                                            class_and_credits = fall_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                fall_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(fall_sem) > 0:
                                                        fall_sem.pop(len(fall_sem) - 1)
                                                    first_choice = fall_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = fall_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    fall_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1

                                        # do the same thing with spring classes
                                        index = 0
                                        while index < len(spring_classes):
                                            class_and_credits = spring_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                spring_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(spring_sem) > 0:
                                                        spring_sem.pop(
                                                            len(spring_sem) - 1
                                                        )
                                                    first_choice = spring_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = spring_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    spring_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1
                                        # add all the courses into the requirements
                                        classes_and_requirements[degree[0]][
                                            "First Year"
                                        ]["Fall"] = fall_sem
                                        classes_and_requirements[degree[0]][
                                            "First Year"
                                        ]["Spring"] = spring_sem
                                        # print(classes_and_requirements)
                                    elif i == 1:
                                        # print("Sophomore year loading")
                                        index = 0
                                        while index < len(fall_classes):
                                            class_and_credits = fall_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = (
                                                    class_and_credits[0 : test - 13]
                                                    .replace("….", "... -")
                                                    .replace("….", "... -")
                                                )
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                fall_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(fall_sem) > 0:
                                                        fall_sem.pop(len(fall_sem) - 1)
                                                    first_choice = fall_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = fall_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    fall_sem.append(or_classes)
                                                    index += 2
                                                    # testign
                                                else:
                                                    index += 1
                                        # do the same thing with spring classes
                                        index = 0
                                        while index < len(spring_classes):
                                            class_and_credits = spring_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                spring_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(spring_sem) > 0:
                                                        spring_sem.pop(
                                                            len(spring_sem) - 1
                                                        )
                                                    first_choice = spring_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = spring_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    spring_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1
                                        # add all the courses into the requirements
                                        classes_and_requirements[degree[0]][
                                            "Second Year"
                                        ]["Fall"] = fall_sem
                                        classes_and_requirements[degree[0]][
                                            "Second Year"
                                        ]["Spring"] = spring_sem
                                        # print(classes_and_requirements)
                                        # sys.exit(1)

                                    # TO DO
                                    # Arch semesters are different. Approach must be different.
                                    elif i == 2:
                                        # print(arch_classes)
                                        # print(arch_sem)
                                        # print(fall_classes)
                                        # print(spring_classes)
                                        # return
                                        index = 0
                                        while index < len(arch_classes):
                                            class_and_credits = arch_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                arch_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(arch_sem) > 0:
                                                        arch_sem.pop(len(arch_sem) - 1)

                                                    first_choice = arch_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = arch_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    arch_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1
                                        index = 0
                                        while index < len(fall_classes):
                                            class_and_credits = fall_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                fall_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(fall_sem) > 0:
                                                        fall_sem.pop(len(fall_sem) - 1)
                                                    first_choice = fall_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = fall_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    fall_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1
                                        classes_and_requirements[degree[0]][
                                            "Third Year"
                                        ]["Arch"] = arch_sem
                                        classes_and_requirements[degree[0]][
                                            "Third Year"
                                        ]["Fall"] = fall_sem
                                        classes_and_requirements[degree[0]][
                                            "Third Year"
                                        ]["Spring"] = [
                                            "ILEA 4400 - Independent Learning Experience:0"
                                        ]
                                    else:
                                        index = 0
                                        while index < len(fall_classes):
                                            class_and_credits = fall_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                fall_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(fall_sem) > 0:
                                                        fall_sem.pop(len(fall_sem) - 1)
                                                    first_choice = fall_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = fall_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    fall_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1
                                        # do the same thing with spring classes
                                        index = 0
                                        while index < len(spring_classes):
                                            class_and_credits = spring_classes[
                                                index
                                            ].get_text()
                                            try:
                                                test = class_and_credits.index(":")
                                                # Proceed with the logic if the colon is found
                                                # For example, split the string
                                                class_item = class_and_credits[
                                                    0 : test - 13
                                                ].replace("….", "... -")
                                                credits_per_class = class_and_credits[
                                                    test + 2 : test + 3
                                                ]
                                                spring_sem.append(
                                                    class_item
                                                    + ":"
                                                    + str(credits_per_class)
                                                )
                                                index += 1
                                            except ValueError:
                                                # Skip the item or handle it in case of missing colon
                                                # print("Colon not found, skipping this entry.")
                                                if class_and_credits == "or":
                                                    or_classes = []
                                                    if len(spring_sem) > 0:
                                                        spring_sem.pop(
                                                            len(spring_sem) - 1
                                                        )
                                                    first_choice = spring_classes[
                                                        index - 1
                                                    ].get_text()

                                                    test = first_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = first_choice[
                                                        0 : test - 13
                                                    ]
                                                    credits_per_class = first_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )

                                                    second_choice = spring_classes[
                                                        index + 1
                                                    ].get_text()
                                                    test = second_choice.index(":")
                                                    # Proceed with the logic if the colon is found
                                                    # For example, split the string
                                                    class_item = second_choice[0:test]
                                                    credits_per_class = second_choice[
                                                        test + 2 : test + 3
                                                    ]
                                                    or_classes.append(
                                                        class_item
                                                        + ":"
                                                        + str(credits_per_class)
                                                    )
                                                    spring_sem.append(or_classes)
                                                    index += 2
                                                else:
                                                    index += 1
                                        # add all the courses into the requirements
                                        classes_and_requirements[degree[0]][
                                            "Fourth Year"
                                        ]["Fall"] = fall_sem
                                        classes_and_requirements[degree[0]][
                                            "Fourth Year"
                                        ]["Spring"] = spring_sem
                                # return
                            # print(classes_and_requirements)

            folder_path = "csvs"
            file_name = "bachelors.csv"

            # Ensure the folder exists
            os.makedirs(folder_path, exist_ok=True)

            # Full file path
            file_path = os.path.join(folder_path, file_name)

            with open(file_path, mode="w", newline="") as file:
                writer = csv.writer(file)
                for key, value in classes_and_requirements.items():
                    writer.writerow([f"{key}:{value}"])
        break


if __name__ == "__main__":
    main()
