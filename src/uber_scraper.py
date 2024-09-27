import re
from bs4 import BeautifulSoup
import pendulum


def parse_message_body(data: str) -> dict:
    """Reads the html content of the email and extracts the total value, stars, category, distance and duration.

    Args:
        data (str): The html content of the email as a string.

    Returns:
        dict: A dictionary with the total value, stars, category, distance and duration.

    """

    soup = BeautifulSoup(data, "html.parser")

    total_div = soup.find(string=re.compile(r"Total")).parent
    if total_div and total_div.name == "div":
        total_span = total_div.find_next("span")
        match = re.match(r"R\$\s+(\d+),(\d{2})", total_span.string)
        if not match:
            raise ValueError("Could not find total value")

        total_value = float(f"{match.group(1)}.{match.group(2)}")

    # Reading date
    date_span = soup.find("span", string=re.compile(r"\d{1,2} de \w+ de \d{4}"))
    if not date_span:
        raise ValueError("Could not find the date")
    travel_date = pendulum.from_format(
        date_span.string.replace(" de ", " "), "DD MMMM YYYY", locale="pt_BR"
    ).date()

    tr_tag = soup.find(
        lambda x: x.name == "tr"
        and len(x.find_all("td")) == 3
        and x.find_all("td")[2].string == "Avaliação"
    )
    if not tr_tag:
        raise ValueError("Could not find the tr tag with the stars")
    stars = float(tr_tag.find("td").string.replace(",", "."))

    tr_tag = soup.find(
        lambda x: x.name == "tr"
        and len(x.find_all("td")) == 2
        and x.find_all("td")[1].string
        and x.find_all(
            "td", string=re.compile(r"(\d+\.\d+)\s+Quilômetros\s+\|\s+(\d+)\s+min")
        )
    )

    category = next(tr_tag.find("td").stripped_strings)
    distance, duration = re.match(
        r"(\d+\.\d+)\s+Quilômetros\s+\|\s+(\d+)\s+min", tr_tag.find_all("td")[1].string
    ).groups()
    distance = float(distance)
    duration = int(duration)

    return {
        "date": travel_date,
        "total": total_value,
        "stars": stars,
        "category": category,
        "distance": distance,
        "duration": duration,
    }
