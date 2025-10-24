# A Modern Web Scraper for the RPI Student Information System (SIS)

This repository contains source code for a web scraper designed to fetch course data from Rensselaer Polytechnic Institute's new SIS, which replaced the old SIS on September 17th, 2025.

The scraper operates within the student registration system, which is accessible through [this link](https://sis9.rpi.edu/StudentRegistrationSsb/ssb/registration) without needing authentication.

## Data Collected

This scraper is designed to fetch course data from any range of years and academic terms, with Summer 1998 being the earliest available term.

For each academic term scraped, data is outputted as a JSON file. A sample JSON format is shown below:

```json
[
    "CSCI": {
        "subject_name": "Computer Science",
        "courses": {
            "CSCI 1100": {
                "course_name": "Computer Science I",
                "course_detail": {
                    "description": "Course description.",
                    "corequisite": [
                        "SUBJ 1100",
                        "SUBJ 1200"
                    ],
                    "prerequisite": {},
                    "crosslist": [
                        "SUBJ 2010",
                        "SUBJ 2020"
                    ],
                    "attributes": [
                        "DI1",
                        "FRSH"
                    ],
                    "restrictions": {
                        "major": [
                            "Computer Science"
                        ],
                        "not_major": [],
                        "minor": [
                            "Electronic Arts"
                        ],
                        "not_minor": [],
                        "level": [
                            "Graduate"
                        ],
                        "not_level": [],
                        "classification": [
                            "Freshman",
                            "Sophomore"
                        ],
                        "not_classification": [],
                        "degree": [
                            ""
                        ],
                        "not_degree": [],
                        "department": [
                            ""
                        ],
                        "not_department": [],
                        "campus": [
                            "Troy"
                        ],
                        "not_campus": [],
                        "college": [
                            "School of Science (S)"
                        ],
                        "not_college": [],
                        "special_approval": [
                            "Instructor's Approval"
                        ]
                    },
                    "credits": {
                        "min": 4,
                        "max": 0
                    },
                    "sections": [
                        {
                            "CRN": "12345",
                            "instructor": [
                                "rcsid1",
                                "rcsid2"
                            ],
                            "schedule": {},
                            "capacity": 30,
                            "registered": 29,
                            "open": 1
                        },
                        ...
                    ]
                }
            },
            ...
        }
    },
    ...
]
```
