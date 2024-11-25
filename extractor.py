import os, time, pathlib
import requests as rq
from requests.exceptions import RequestException
import polars as pl
from dotenv import load_dotenv
import tenacity
from itertools import combinations

@tenacity.retry(
    retry=tenacity.retry_if_exception_type(RequestException),
    wait=tenacity.wait_exponential(multiplier=5, min=4, max=16),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
def request(issn, from_date, to_date, page=1, per_page=200, mail_to="you@example.com"):
    main_results = []
    referenced_works = []
    citing_works = []
    results = []

    print("Request method has been called!")
    print("Wait for few minutes")

    while True:
        r = rq.get(
            f"https://api.openalex.org/works?page={page}&per-page={per_page}&filter=primary_location.source.issn:{issn},from_publication_date:{from_date},to_publication_date:{to_date}&mailto={mail_to}"
        )
        r.raise_for_status()
        main_results.extend(r.json()["results"])
        if page * per_page >= r.json()["meta"]["count"]:
            break
        page += 1

    results.extend(main_results)

    # Initialize new data structures
    keywords_data = []
    all_referenced_works = []

    for work in main_results:
        work_id = work["id"].split("/")[-1]
        work_title = work["title"]

        # Extract keywords
        if "keywords" in work:
            keywords_data.append(
                {
                "work_id": work_id,
                "keywords": ", ".join(
                [keyword["display_name"] for keyword in work.get("keywords", [])]
            ),
        }
            )

        # Extract referenced works
        if "referenced_works" in work:
            top_referenced_works = work["referenced_works"][:3]
            all_referenced_works.extend(
                [{"work_id": work_id, "work_title" : work_title, "referenced_work_id": ref.split("/")[-1], "referenced_url": "https://api.openalex.org/works/" + ref.split("/")[-1]}
                 for ref in top_referenced_works]
            )

    print("Fetching titles for referenced works and writing additional data into files")

    # Add titles for referenced works
    referenced_data_by_work = {}

    for index, ref_work in enumerate(all_referenced_works, start=1):
        WORKID = ref_work["work_id"]
        referenced_work_id = ref_work["referenced_work_id"]
        ref_url = ref_work["referenced_url"]
        work_title_print = ref_work["work_title"]
        
        # Fetch title, source, and authors
        title = fetch_title_from_url(ref_url)
        source, authors = fetch_source_and_authors(ref_url)

        print(f"{index} - Work_id: {WORKID} | Work Title: {work_title_print} | Reference -> ID : {referenced_work_id} | Title: {title} | Source: {source} | Authors: {authors}")
        
        # Group data by work_id
        if WORKID not in referenced_data_by_work:
            # Extract the main work source and authors from the work_id
            main_work = next(work for work in main_results if work["id"].split("/")[-1] == WORKID)
            work_source = (
                main_work["primary_location"]["source"]["display_name"]
                if main_work.get("primary_location") and main_work["primary_location"].get("source")
                else "Source Not Found"
            )
            work_authors = ", ".join(
                [
                    authorship["author"]["display_name"]
                    for authorship in main_work.get("authorships", [])
                ]
            ) if main_work.get("authorships") else "Authors Not Found"

            referenced_data_by_work[WORKID] = {
                "work_id": WORKID,
                "work_title" : work_title_print,
                "work_source": work_source,
                "work_authors": work_authors,
                "referenced_titles": [],
                "referenced_sources": [],
                "referenced_authors": [],
                "referenced_work_ids": []
            }
        
        referenced_data_by_work[WORKID]["referenced_titles"].append(title)
        referenced_data_by_work[WORKID]["referenced_sources"].append(source)
        referenced_data_by_work[WORKID]["referenced_authors"].append(authors)
        referenced_data_by_work[WORKID]["referenced_work_ids"].append(referenced_work_id)

    # Flatten the grouped data
    flattened_referenced_data = [
        {
            "work_id": work_id,
            "work_title": data["work_title"],
            "work_source": data["work_source"],
            "work_authors": data["work_authors"],
            "referenced_work_ids": safe_join(data["referenced_work_ids"], default="Unknown Work ID"),
            "referenced_titles": safe_join(data["referenced_titles"], default="Unknown Title"),
            "referenced_sources": safe_join(data["referenced_sources"], default="Unknown Source"),
            "referenced_authors": safe_join(data["referenced_authors"], default="Unknown Author")
        }
        for work_id, data in referenced_data_by_work.items()
    ]

    # Save to CSV with all referenced data in one row per work
    referenced_works_df = pl.DataFrame(flattened_referenced_data).sort("work_id")
    referenced_works_df.write_csv("new-data/referenced_works.csv")

    if keywords_data:
        print("Sample keywords_data:", keywords_data[:5])

    # Create DataFrame and sort
    keywords_df = pl.DataFrame(keywords_data)

    # Check if DataFrame contains the required columns
    print("Columns in keywords_df:", keywords_df.columns)

    # Ensure sorting on existing columns
    keywords_df = keywords_df.unique().sort(["work_id", "keywords"])
    keywords_df.write_csv("new-data/keywords.csv")

    # Compile and save the main works data
    works = [
        {
            "id": work["id"].split("/")[-1],
            "doi": work["doi"],
            "openalex_url": work["id"],
            "title": work["title"],
            "publication_date": work["publication_date"],
            "publication_year": work["publication_year"],
            "volume": work["biblio"]["volume"],
            "issue": work["biblio"]["issue"],
            "type": work["type"],
            "source": (
                work["primary_location"]["source"]["display_name"]
                if work.get("primary_location", None)
                and work["primary_location"].get("source", None)
                else None
            ),
            "source_orginization": (
                work["primary_location"]["source"]["host_organization_name"]
                if work.get("primary_location", None)
                and work["primary_location"].get("source", None)
                else None
            ),
            "source_type": (
                work["primary_location"]["source"]["type"]
                if work.get("primary_location", None)
                and work["primary_location"].get("source", None)
                else None
            ),
            "citation_count": work["cited_by_count"],
            "reference_count": work["referenced_works_count"],
            # Include keywords as a comma-separated list
            "keywords": ", ".join(
                [keyword["display_name"] for keyword in work.get("keywords", [])]
            ),
        }
        for work in results
    ]

    print("Writing works data into file")

    works_df = pl.DataFrame(works)
    works_df = works_df.unique().sort(
        [
            "source_type",
            "source_orginization",
            "source",
            "publication_year",
            "volume",
            "issue",
        ]
    )
    works_df.write_csv("new-data/works.csv")

    # Save all the existing and new components
    save_existing_components(results)

# Helper function to fetch titles from OpenAlex URLs
@tenacity.retry(
    retry=tenacity.retry_if_exception_type(RequestException),
    wait=tenacity.wait_exponential(multiplier=5, min=4, max=16),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)

# Helper function to safely join a list of strings, replacing None with a default value
def safe_join(items, default="Unknown"):
    return ", ".join([item if item is not None else default for item in items])

def fetch_source_and_authors(ref_url):
    try:
        response = rq.get(ref_url)
        response.raise_for_status()
        data = response.json()
        
        # Safely check and extract source information
        source_name = "Source Not Found"
        if data and data.get("primary_location") and data["primary_location"].get("source"):
            source_name = data["primary_location"]["source"].get("display_name", "Source Not Found")
        
        # Safely check and extract author names
        authorship_list = data.get("authorships", []) if data else []
        authors = [
            authorship.get("author", {}).get("display_name", "Unknown Author")
            for authorship in authorship_list
        ]
        author_names = ", ".join(authors) if authors else "Authors Not Found"
        
        return source_name, author_names
        
    except RequestException as e:
        print(f"Failed to fetch source and authors for URL {ref_url}: {str(e)}")
        return "Error Fetching Source", "Error Fetching Authors"

def fetch_title_from_url(ref_url):
    try:
        response = rq.get(ref_url)
        response.raise_for_status()
        data = response.json()
        return data.get("title", "Title Not Found")
    except RequestException as e:
        print(f"Failed to fetch title for URL {ref_url}: {str(e)}")
        return "Error Fetching Title"
    
def save_existing_components(results):
    # Topics
    topics = [
        {
            "work_id": work["id"].split("/")[-1],
            "topic": topic["display_name"],
            "subfield": topic["subfield"]["display_name"],
            "field": topic["field"]["display_name"],
            "domain": topic["domain"]["display_name"],
        }
        for work in results
        for topic in work["topics"]
    ]
    topics_df = pl.DataFrame(topics).unique().sort(["work_id"])
    topics_df.write_csv("new-data/topics.csv")

    # Yearly citations
    yearly_citations = [
        {
            "work_id": work["id"].split("/")[-1],
            "year": yearly_count["year"],
            "citation_count": yearly_count["cited_by_count"],
        }
        for work in results
        for yearly_count in work["counts_by_year"]
    ]
    yearly_citations_df = pl.DataFrame(yearly_citations).unique().sort(["work_id", "year"])
    yearly_citations_df.write_csv("new-data/yearly_citations.csv")

    # Authors
    authors = [
        {
            "id": authorship["author"]["id"].split("/")[-1],
            "name": authorship["author"]["display_name"],
            "affiliation": (
                authorship["raw_affiliation_strings"][0]
                if authorship.get("raw_affiliation_strings", None)
                and len(authorship["raw_affiliation_strings"]) > 0
                else None
            ),
        }
        for work in results
        for authorship in work["authorships"]
    ]
    authors_df = pl.DataFrame(authors).unique().sort(["name", "id"])
    authors_df.write_csv("new-data/authors.csv")

    # Works-Authors
    works_authors = [
        {
            "work_id": work["id"].split("/")[-1],
            "author_id": authorship["author"]["id"].split("/")[-1],
            "position": authorship["author_position"],
        }
        for work in results
        for authorship in work["authorships"]
    ]
    works_authors_df = pl.DataFrame(works_authors).unique().sort(["work_id", "author_id"])
    works_authors_df.write_csv("new-data/works_authors.csv")

    # Institutions
    institutions = [
        {
            "id": institution["id"].split("/")[-1],
            "name": institution["display_name"],
            "country_code": institution["country_code"],
            "type": institution["type"],
        }
        for work in results
        for authorship in work["authorships"]
        for institution in authorship["institutions"]
    ]
    institutions_df = pl.DataFrame(institutions).unique().sort(["name", "id"])
    institutions_df.write_csv("new-data/institutions.csv")

    # Works-Authors-Institutions
    works_authors_institutions = [
        {
            "work_id": work["id"].split("/")[-1],
            "author_id": authorship["author"]["id"].split("/")[-1],
            "institution_id": institution["id"].split("/")[-1],
        }
        for work in results
        for authorship in work["authorships"]
        for institution in authorship["institutions"]
    ]
    works_authors_institutions_df = pl.DataFrame(works_authors_institutions).unique().sort(
        ["work_id", "author_id", "institution_id"]
    )
    works_authors_institutions_df.write_csv("new-data/works_authors_institutions.csv")


def main():
    load_dotenv()
    env = os.environ
    if not (env.get("ISSN") and env.get("FROM") and env.get("TO")):
        raise Exception("Environment variables not loaded correctly.")

    pathlib.Path("new-data").mkdir(exist_ok=True)
    request(
        env.get("ISSN", ""),
        env.get("FROM", "2000-01-01"),
        env.get("TO", "2023-12-31"),
        mail_to=env.get("EMAIL"),
    )


if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"{'-'*10}Extraction complete{'-'*10}")
    print(f"Elapsed time: {time.time()-start_time} seconds")