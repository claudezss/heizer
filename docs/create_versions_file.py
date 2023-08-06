import json
import pathlib

import requests


def create():
    versions = [{"name": "main", "url": "https://heizer.claudezss.com/docs/main"}]

    rsp = requests.get("https://api.github.com/repos/claudezss/heizer/releases?per_page=100")
    d = rsp.json()
    for item in d:
        release_name = item["name"]
        versions.append({"name": release_name, "url": f"https://heizer.claudezss.com/docs/{release_name}/"})

    with open(pathlib.Path(__file__).parent / "versions.json", "+w") as f:
        json.dump(versions, f, indent=4)


if __name__ == "__main__":
    create()
